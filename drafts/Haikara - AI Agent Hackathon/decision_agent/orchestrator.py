from flask import Flask, jsonify
import requests
import random

GRID_AGENT_URL = "http://127.0.0.1:5002"

app = Flask(__name__)

def mock_carbon_intensity():
    return random.randint(80, 260)

def mock_price():
    return round(random.uniform(0.12, 0.43), 3)

CARBON_CAP = 150

@app.route("/run_decision", methods=["GET"])
def run_decision():
    carbon = mock_carbon_intensity()
    price = mock_price()

    # Trigger Beckn /search if grid is stressed
    if carbon > CARBON_CAP or price > 0.30:
        search_res = requests.post(f"{GRID_AGENT_URL}/search").json()
        slots = search_res.get("slots", [])

        if not slots:
            return jsonify({"decision": "No slots available"}), 200

        # Pick a low-energy slot
        best_slot = sorted(slots, key=lambda s: s["energy_kwh"])[0]
        slot_id = best_slot["id"]

        requests.post(f"{GRID_AGENT_URL}/select", json={"slot_id": slot_id})
        confirm_res = requests.post(f"{GRID_AGENT_URL}/confirm", json={"slot_id": slot_id}).json()

        return jsonify({
            "carbon": carbon,
            "price": price,
            "selected_slot": best_slot,
            "confirm": confirm_res
        }), 200

    return jsonify({"decision": "Run now", "carbon": carbon, "price": price}), 200

if __name__ == "__main__":
    app.run(port=5003)
