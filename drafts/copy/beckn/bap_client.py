import aiohttp
import json
import hashlib
import base64
from datetime import datetime
from typing import Dict, List
import uuid

class BAPClient:
    """
    Beckn Application Platform (BAP) Client
    Handles all Beckn protocol communications
    """
    
    def __init__(self, bap_url: str, subscriber_id: str, private_key: str):
        self.bap_url = bap_url
        self.subscriber_id = subscriber_id
        self.private_key = private_key
        
        print(f"🔗 Beckn BAP Client initialized")
        print(f"   BAP URL: {bap_url}")
        print(f"   Subscriber ID: {subscriber_id}")
    
    def _build_context(self, action: str, transaction_id: str = None) -> dict:
        """Build Beckn context for request"""
        return {
            "domain": "energy:compute",
            "action": action,
            "version": "1.1.0",
            "bap_id": self.subscriber_id,
            "bap_uri": self.bap_url,
            "transaction_id": transaction_id or str(uuid.uuid4()),
            "message_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "ttl": "PT30M"
        }
    
    def _sign_payload(self, payload: dict) -> str:
        """
        Create cryptographic signature for Beckn authentication
        In production, use proper Ed25519 or RSA signing
        """
        payload_str = json.dumps(payload, sort_keys=True)
        signature = hashlib.sha256(payload_str.encode()).hexdigest()
        return signature
    
    async def _make_request(self, endpoint: str, payload: dict) -> dict:
        """Make HTTP request to BAP Sandbox"""
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Signature {self._sign_payload(payload)}"
        }
        
        url = f"{self.bap_url}/{endpoint}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_text = await response.text()
                        print(f"❌ Beckn request failed: {response.status} - {error_text}")
                        return {"error": error_text, "status": response.status}
        except asyncio.TimeoutError:
            print(f"⚠️  Beckn request timeout for {endpoint}")
            return {"error": "timeout"}
        except Exception as e:
            print(f"❌ Beckn request error: {e}")
            return {"error": str(e)}
    
    async def search(self, intent: dict, transaction_id: str = None) -> dict:
        """
        Send /search request to discover compute slots
        """
        payload = {
            "context": self._build_context("search", transaction_id),
            "message": {
                "intent": intent
            }
        }
        
        print(f"🔍 Beckn SEARCH: {intent.get('item', {}).get('descriptor', {}).get('name', 'compute')}")
        return await self._make_request("search", payload)
    
    async def select(self, provider_id: str, item_id: str, transaction_id: str) -> dict:
        """
        Send /select request to choose specific offer
        """
        payload = {
            "context": self._build_context("select", transaction_id),
            "message": {
                "order": {
                    "provider": {"id": provider_id},
                    "items": [{"id": item_id}]
                }
            }
        }
        
        print(f"✅ Beckn SELECT: Provider {provider_id}, Item {item_id}")
        return await self._make_request("select", payload)
    
    async def init(self, order_details: dict, transaction_id: str) -> dict:
        """
        Send /init request to initialize order
        """
        payload = {
            "context": self._build_context("init", transaction_id),
            "message": {
                "order": order_details
            }
        }
        
        print(f"🔧 Beckn INIT: Order initialization")
        return await self._make_request("init", payload)
    
    async def confirm(self, order_details: dict, transaction_id: str) -> dict:
        """
        Send /confirm request to confirm order
        """
        payload = {
            "context": self._build_context("confirm", transaction_id),
            "message": {
                "order": order_details
            }
        }
        
        print(f"✔️  Beckn CONFIRM: Order confirmed")
        return await self._make_request("confirm", payload)
    
    async def status(self, order_id: str, transaction_id: str) -> dict:
        """
        Send /status request to check order status
        """
        payload = {
            "context": self._build_context("status", transaction_id),
            "message": {
                "order_id": order_id
            }
        }
        
        return await self._make_request("status", payload)
    
    async def update(self, order_id: str, updates: dict, transaction_id: str) -> dict:
        """
        Send /update request to modify order
        """
        payload = {
            "context": self._build_context("update", transaction_id),
            "message": {
                "order": {
                    "id": order_id,
                    **updates
                }
            }
        }
        
        print(f"🔄 Beckn UPDATE: Order {order_id}")
        return await self._make_request("update", payload)


class BecknOrchestrator:
    """
    Orchestrates complete Beckn transaction workflows
    Manages the full lifecycle: search -> select -> init -> confirm -> status
    """
    
    def __init__(self, bap_url: str, subscriber_id: str, private_key: str):
        self.client = BAPClient(bap_url, subscriber_id, private_key)
        self.active_transactions = {}
    
    async def execute_schedule(self, schedule: List[dict]):
        """
        Execute optimized schedule via Beckn protocol
        Converts schedule into Beckn transactions
        """
        
        print(f"\n🔗 Executing {len(schedule)} jobs via Beckn...")
        
        for scheduled_job in schedule:
            try:
                await self._execute_single_job(scheduled_job)
            except Exception as e:
                print(f"❌ Failed to execute job {scheduled_job['job'].id}: {e}")
    
    async def _execute_single_job(self, scheduled_job: dict):
        """Execute single job through complete Beckn workflow"""
        
        job = scheduled_job['job']
        transaction_id = str(uuid.uuid4())
        
        print(f"\n  📦 Job {job.id} ({job.job_type}):")
        
        # Step 1: SEARCH for compute slots
        search_intent = {
            "item": {
                "descriptor": {"name": "compute_slot"}
            },
            "fulfillment": {
                "time": {
                    "range": {
                        "start": scheduled_job['start_datetime'].isoformat(),
                        "end": scheduled_job['end_datetime'].isoformat()
                    }
                }
            },
            "location": {
                "region": scheduled_job['region']
            },
            "tags": [
                {"descriptor": {"code": "energy_kwh"}, "value": str(job.energy_kwh)},
                {"descriptor": {"code": "cpu_cores"}, "value": str(job.cpu_cores)},
                {"descriptor": {"code": "gpu_count"}, "value": str(job.gpu_count)}
            ]
        }
        
        search_response = await self.client.search(search_intent, transaction_id)
        
        if "error" in search_response:
            print(f"    ❌ Search failed: {search_response['error']}")
            return
        
        print(f"    ✓ Search complete")
        
        # Step 2: SELECT (simulate provider selection)
        # In production, this would parse on_search callback
        provider_id = "mock_provider_" + scheduled_job['region']
        item_id = f"compute_slot_{transaction_id[:8]}"
        
        select_response = await self.client.select(provider_id, item_id, transaction_id)
        print(f"    ✓ Select complete")
        
        # Step 3: INIT order
        order_details = {
            "provider": {"id": provider_id},
            "items": [{"id": item_id, "quantity": {"selected": {"count": job.cpu_cores}}}],
            "billing": {
                "name": "Flexible Compute Scheduler",
                "email": "billing@flexcompute.ai"
            }
        }
        
        init_response = await self.client.init(order_details, transaction_id)
        print(f"    ✓ Init complete")
        
        # Step 4: CONFIRM order
        confirm_details = {
            **order_details,
            "status": "ACTIVE",
            "payment": {
                "type": "POST-FULFILLMENT",
                "status": "NOT-PAID"
            }
        }
        
        confirm_response = await self.client.confirm(confirm_details, transaction_id)
        
        if "error" not in confirm_response:
            order_id = f"order_{transaction_id[:12]}"
            self.active_transactions[order_id] = {
                'job': job,
                'transaction_id': transaction_id,
                'scheduled': scheduled_job,
                'status': 'CONFIRMED',
                'confirmed_at': datetime.now()
            }
            print(f"    ✅ Confirmed - Order ID: {order_id}")
        else:
            print(f"    ❌ Confirm failed: {confirm_response['error']}")