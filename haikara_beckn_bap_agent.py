"""
Haikara - Beckn BAP Sandbox Multi-Agent Prototype
Single-file FastAPI app implementing:
 - Compute Agent (as part of BAP caller behavior)
 - Grid Agent (simulated stream)
 - Decision (Orchestrator) Agent
 - Audit Agent (logging + persistence)

Features:
 - Sends Beckn caller requests to BAP sandbox endpoints (/bap/caller/*)
 - Implements the required on_* callback endpoints so sandbox can reply asynchronously
 - Implements the Beckn flows: search/select/init/confirm/update/status/rating/support
 - Simple short-horizon forecasting + greedy optimizer for scheduling
 - File-based audit logs including transaction IDs, timestamps, signatures (mocked)

How to run:
 1) pip install fastapi uvicorn httpx pydantic
 2) export BAP_BASE_URL=http://127.0.0.1:8080  # change to your BAP sandbox host
 3) python haikara_beckn_bap_agent.py

Note: This is a prototype. Replace mocked crypto, forecasting and real feed with production components.
"""

from fastapi import FastAPI, Request, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from uuid import uuid4
from datetime import datetime, timedelta
import httpx
import os
import json
import asyncio
import logging
from dateutil import parser
from datetime import timezone
from dateutil.parser import isoparse


# Basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("haikara")

app = FastAPI(title="Haikara Beckn BAP Agent Prototype")

# BAP sandbox base URL (the agent will post to these endpoints)
BAP_BASE = os.getenv("BAP_BASE_URL", "http://127.0.0.1:8080")

# Audit store path
AUDIT_FILE = os.getenv("HAIKARA_AUDIT_FILE", "haikara_audit_log.jsonl")

# Simple in-memory state stores (also persisted to audit)
ORDERS: Dict[str, Dict[str, Any]] = {}
SEARCH_REQUESTS: Dict[str, Dict[str, Any]] = {}

# --------------------------- Utilities ---------------------------

def now_iso():
    return datetime.utcnow().isoformat() + "Z"


def audit(event_type: str, payload: Dict[str, Any]):
    record = {
        "ts": now_iso(),
        "event": event_type,
        "payload": payload
    }
    # append to file
    with open(AUDIT_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")
    logger.info("AUDIT %s %s", event_type, payload.get("id") or "",)


def mock_sign(data: Dict[str, Any]) -> str:
    # Mock cryptographic signature -- replace with real signing for production
    return f"sig_{hash(json.dumps(data, sort_keys=True)) & 0xffffffff:08x}"


# --------------------------- Models ---------------------------

class GridSearchCriteria(BaseModel):
    region: str
    earliest_start: datetime
    latest_end: datetime
    capacity_mw: float
    max_carbon_gco2_per_kwh: float


class BecknRequest(BaseModel):
    context: Dict[str, Any]
    message: Dict[str, Any]


# --------------------------- Mock Grid Agent (simulator) ---------------------------

# For prototype, we simulate a grid stream and grid windows
SIMULATED_GRID_WINDOWS = []

def generate_simulated_grid_windows(now: datetime, horizon_hours=24):
    windows = []
    for i in range(horizon_hours):
        start = (now + timedelta(hours=i)).replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        end = start + timedelta(hours=1)
        # simple alternating carbon/price pattern
        carbon = 200 - (i % 6) * 20  # gCO2/kWh fluctuating
        price = 0.20 - (i % 6) * 0.01
        renewable = max(10, 50 + ((i+3) % 6) * 8)
        windows.append({
            "window_id": f"gw-{start.isoformat()}",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "carbon_gco2": carbon,
            "price_per_kwh": round(max(0.01, price), 4),
            "renewable_pct": renewable,
            "available_mw": 10.0
        })
    return windows

# populate simulated windows at startup
SIMULATED_GRID_WINDOWS = generate_simulated_grid_windows(isoparse("2025-11-24T00:00:00Z"), horizon_hours=48)

# --------------------------- Decision / Orchestrator Logic ---------------------------

async def short_horizon_forecast(grid_windows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # For prototype: forecasting simply returns the same windows but with a small noise
    forecasts = []
    for w in grid_windows:
        f = w.copy()
        # naive forecast: carbon may increase a bit in next hour
        f["forecasted_carbon_gco2"] = max(0, w["carbon_gco2"] + (2 - (hash(w['window_id']) % 5)))
        f["forecasted_price_per_kwh"] = round(max(0.01, w["price_per_kwh"] * (1 + ((hash(w['window_id']) % 3)-1)*0.02)),4)
        forecasts.append(f)
    return forecasts


def optimize_schedule(workloads: List[Dict[str, Any]], forecasts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Greedy optimizer: for each workload, pick the lowest cost window within its window
    plan = []
    for job in workloads:
        candidate_windows = []
        for w in forecasts:
            # check time window intersection
            w_start = parser.isoparse(w['start'])
            w_end = parser.isoparse(w['end'])
            if w_start >= job['earliest_start'] and w_end <= job['latest_end'] and w['available_mw'] >= job['required_mw']:
                candidate_windows.append(w)
        if not candidate_windows:
            # no match: pick the earliest that fits
            plan.append({"job_id": job['job_id'], "window_id": None, "action": "defer"})
            continue
        # sort by price then carbon then renewable
        candidate_windows.sort(key=lambda x: (x['forecasted_price_per_kwh'], x['forecasted_carbon_gco2'], -x['renewable_pct']))
        chosen = candidate_windows[0]
        plan.append({
            "job_id": job['job_id'],
            "window_id": chosen['window_id'],
            "start": chosen['start'],
            "end": chosen['end'],
            "price_per_kwh": chosen['forecasted_price_per_kwh'],
            "carbon_gco2": chosen['forecasted_carbon_gco2'],
            "action": "schedule"
        })
    return plan

# --------------------------- Beckn Caller (BAP -> BAP sandbox) helpers ---------------------------

async def bap_post(path: str, payload: Dict[str, Any]):
    logger.info("Mock POST to %s: %s", path, payload)
    class MockResp:
        status_code = 200
    return MockResp()

# async def bap_post(path: str, payload: Dict[str, Any]) -> httpx.Response:
#     url = BAP_BASE.rstrip('/') + path
#     async with httpx.AsyncClient(timeout=30.0) as client:
#         logger.info("POST %s", url)
#         try:
#             r = await client.post(url, json=payload)
#             logger.info("BAP post status %s", r.status_code)
#             return r
#         except Exception as e:
#             logger.exception("Error posting to BAP %s", e)
#             raise


async def send_search(criteria: GridSearchCriteria) -> str:
    # Compose a Beckn search request (simplified for sandbox)
    req_id = str(uuid4())
    context = {
        "domain": "haikara.compute",
        "action": "search",
        "timestamp": now_iso(),
        "message_id": req_id,
        "sender_id": "haikara_bap_agent"
    }
    message = {
        "criteria": {
            "region": criteria.region,
            "earliest_start": criteria.earliest_start.isoformat(),
            "latest_end": criteria.latest_end.isoformat(),
            "capacity_mw": criteria.capacity_mw,
            "max_carbon": criteria.max_carbon_gco2_per_kwh
        }
    }

    payload = {
        "context": context,
        "message": message
    }
    SEARCH_REQUESTS[req_id] = {"criteria": criteria.dict(), "status": "sent", "ts": now_iso()}
    audit("search_sent", {"id": req_id, "criteria": message['criteria']})
    resp = await bap_post("/bap/caller/search", payload)
    # ack comes back immediately; actual results come to /on_search
    return req_id


async def send_select(select_details: Dict[str, Any]) -> str:
    req_id = str(uuid4())
    context = {"domain": "haikara.compute", "action": "select", "timestamp": now_iso(), "message_id": req_id, "sender_id": "haikara_bap_agent"}
    payload = {"context": context, "message": select_details}
    audit("select_sent", {"id": req_id, "details": select_details})
    await bap_post("/bap/caller/select", payload)
    return req_id


async def send_init(init_details: Dict[str, Any]) -> str:
    req_id = str(uuid4())
    context = {"domain": "haikara.compute", "action": "init", "timestamp": now_iso(), "message_id": req_id, "sender_id": "haikara_bap_agent"}
    payload = {"context": context, "message": init_details}
    audit("init_sent", {"id": req_id, "details": init_details})
    await bap_post("/bap/caller/init", payload)
    return req_id


async def send_confirm(confirm_details: Dict[str, Any]) -> str:
    req_id = str(uuid4())
    context = {"domain": "haikara.compute", "action": "confirm", "timestamp": now_iso(), "message_id": req_id, "sender_id": "haikara_bap_agent"}
    payload = {"context": context, "message": confirm_details}
    audit("confirm_sent", {"id": req_id, "details": confirm_details})
    await bap_post("/bap/caller/confirm", payload)
    return req_id


async def send_update(update_details: Dict[str, Any]) -> str:
    req_id = str(uuid4())
    context = {"domain": "haikara.compute", "action": "update", "timestamp": now_iso(), "message_id": req_id, "sender_id": "haikara_bap_agent"}
    payload = {"context": context, "message": update_details}
    audit("update_sent", {"id": req_id, "details": update_details})
    await bap_post("/bap/caller/update", payload)
    return req_id


async def send_status(status_details: Dict[str, Any]) -> str:
    req_id = str(uuid4())
    context = {"domain": "haikara.compute", "action": "status", "timestamp": now_iso(), "message_id": req_id, "sender_id": "haikara_bap_agent"}
    payload = {"context": context, "message": status_details}
    audit("status_sent", {"id": req_id, "details": status_details})
    await bap_post("/bap/caller/status", payload)
    return req_id


async def send_rating(rating_details: Dict[str, Any]) -> str:
    req_id = str(uuid4())
    context = {"domain": "haikara.compute", "action": "rating", "timestamp": now_iso(), "message_id": req_id, "sender_id": "haikara_bap_agent"}
    payload = {"context": context, "message": rating_details}
    audit("rating_sent", {"id": req_id, "details": rating_details})
    await bap_post("/bap/caller/rating", payload)
    return req_id


async def send_support(support_details: Dict[str, Any]) -> str:
    req_id = str(uuid4())
    context = {"domain": "haikara.compute", "action": "support", "timestamp": now_iso(), "message_id": req_id, "sender_id": "haikara_bap_agent"}
    payload = {"context": context, "message": support_details}
    audit("support_sent", {"id": req_id, "details": support_details})
    await bap_post("/bap/caller/support", payload)
    return req_id

# --------------------------- on_* callback endpoints (BAP consumer callbacks) ---------------------------

@app.post("/on_search")
async def on_search(req: Request):
    body = await req.json()
    # sandbox will POST results here for earlier search requests
    logger.info("on_search received: %s", body.get('context', {}).get('message_id'))
    audit("on_search_received", body)
    # store or process discovered grid windows
    message = body.get('message', {})
    search_id = body.get('context', {}).get('message_id')
    SEARCH_REQUESTS.setdefault(search_id, {})['results'] = message.get('items', [])
    SEARCH_REQUESTS[search_id]['status'] = 'results_received'
    # You can trigger decision agent to re-run optimization here if needed

    return {"ack": True}


@app.post("/on_select")
async def on_select(req: Request):
    body = await req.json()
    audit("on_select_received", body)
    # handle selection acknowledgement or negotiation
    return {"ack": True}


@app.post("/on_init")
async def on_init(req: Request):
    body = await req.json()
    audit("on_init_received", body)
    # store provisional order id
    message = body.get('message', {})
    order_id = message.get('order', {}).get('id') or str(uuid4())
    ORDERS[order_id] = {"status": "initiated", "body": body}
    return {"ack": True}


@app.post("/on_confirm")
async def on_confirm(req: Request):
    body = await req.json()
    audit("on_confirm_received", body)
    message = body.get('message', {})
    order = message.get('order', {})
    order_id = order.get('id') or str(uuid4())
    ORDERS[order_id] = {"status": "confirmed", "order": order, "body": body}
    return {"ack": True}


@app.post("/on_update")
async def on_update(req: Request):
    body = await req.json()
    audit("on_update_received", body)
    # keep order updates
    return {"ack": True}


@app.post("/on_status")
async def on_status(req: Request):
    body = await req.json()
    audit("on_status_received", body)
    return {"ack": True}


@app.post("/on_rating")
async def on_rating(req: Request):
    body = await req.json()
    audit("on_rating_received", body)
    return {"ack": True}


@app.post("/on_support")
async def on_support(req: Request):
    body = await req.json()
    audit("on_support_received", body)
    return {"ack": True}

# --------------------------- Public API: submit workloads & run decision cycle ---------------------------

class WorkloadSpec(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    required_mw: float
    duration_minutes: int
    earliest_start: datetime
    latest_end: datetime
    carbon_budget_gco2_per_kwh: float
    priority: int = 5
    type: str = "batch"


@app.post("/submit_workloads")
async def submit_workloads(workloads: List[WorkloadSpec], background: BackgroundTasks):
    # store workloads and trigger decision cycle in background
    wl_list = [w.dict() for w in workloads]
    audit("workloads_submitted", {"count": len(wl_list), "jobs": [w['job_id'] for w in wl_list]})
    background.add_task(decision_cycle, wl_list)
    return {"accepted": True, "jobs": [w['job_id'] for w in wl_list]}


async def decision_cycle(workloads: List[Dict[str, Any]]):
    # 1) Discover grid windows
    now = datetime.utcnow()
    grid_windows = SIMULATED_GRID_WINDOWS  # in real system, grid agent would stream these
    audit("grid_windows_observed", {"count": len(grid_windows)})

    # 2) Forecast
    forecasts = await short_horizon_forecast(grid_windows)
    audit("forecasts_generated", {"count": len(forecasts)})

    # 3) Optimize schedule
    plan = optimize_schedule(workloads, forecasts)
    audit("plan_generated", {"plan": plan})

    # 4) For each scheduled job send Beckn flows: search -> select -> init -> confirm
    for p in plan:
        job_id = p['job_id']
        if p['action'] != 'schedule' or not p.get('window_id'):
            audit("job_deferred", {"job_id": job_id})
            continue
        # 4.1 search: find the grid window (we'll craft a criteria)
        criteria = GridSearchCriteria(
            region="region-1",
            earliest_start=datetime.fromisoformat(p['start']),
            latest_end=datetime.fromisoformat(p['end']),
            capacity_mw=workloads[0]['required_mw'],
            max_carbon_gco2_per_kwh=workloads[0]['carbon_budget_gco2_per_kwh']
        )
        search_id = await send_search(criteria)
        # Wait briefly to allow sandbox to callback with on_search (in real system this is async)
        await asyncio.sleep(0.5)

        # 4.2 select
        select_details = {
            "search_id": search_id,
            "selected_window_id": p['window_id'],
            "requested_capacity_mw": workloads[0]['required_mw'],
            "job_id": job_id
        }
        select_id = await send_select(select_details)
        await asyncio.sleep(0.2)

        # 4.3 init
        init_details = {
            "order": {
                "id": f"order-{job_id}",
                "job_id": job_id,
                "window_id": p['window_id'],
                "required_mw": workloads[0]['required_mw'],
                "duration_minutes": workloads[0]['duration_minutes'],
                "type": workloads[0]['type']
            }
        }
        init_id = await send_init(init_details)
        await asyncio.sleep(0.2)

        # 4.4 confirm
        confirm_details = {"order_id": f"order-{job_id}", "accept": True}
        confirm_id = await send_confirm(confirm_details)
        # store local order record
        ORDERS[f"order-{job_id}"] = {"job_id": job_id, "window_id": p['window_id'], "status": "confirmed", "price": p['price_per_kwh']}
        audit("order_created", {"order_id": f"order-{job_id}", "job_id": job_id, "window_id": p['window_id']})

    # 5) Monitoring loop (simulate execution + status updates)
    for order_id, record in ORDERS.items():
        # simulate start event
        status_details = {"order_id": order_id, "status": "started", "ts": now_iso()}
        await send_status(status_details)
        audit("order_started", {"order_id": order_id})
        # simulate running for short while
        await asyncio.sleep(0.1)
        # simulate complete
        status_details = {"order_id": order_id, "status": "completed", "ts": now_iso()}
        await send_status(status_details)
        audit("order_completed", {"order_id": order_id})

    # 6) Finalize audit / settlement metrics (simple mock)
    for order_id, record in ORDERS.items():
        settlement = {
            "order_id": order_id,
            "energy_kwh": record.get('price', 0) * 1000,  # MOCK
            "carbon_gco2": record.get('price', 0) * 1000 * 0.2,
            "revenue": 0
        }
        audit("settlement", settlement)

# --------------------------- Simple endpoints for inspection ---------------------------

@app.get("/state/orders")
async def get_orders():
    return ORDERS

@app.get("/state/searches")
async def get_searches():
    return SEARCH_REQUESTS

@app.get("/state/grid_windows")
async def get_grid_windows():
    return SIMULATED_GRID_WINDOWS

# --------------------------- Run app with uvicorn when invoked directly ---------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("haikara_beckn_bap_agent:app", host="0.0.0.0", port=8000, reload=False)
