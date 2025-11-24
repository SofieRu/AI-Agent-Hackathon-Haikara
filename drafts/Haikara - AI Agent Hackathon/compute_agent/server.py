from flask import Flask, request, jsonify

app = Flask(__name__)

# Example compute job slots (mock)
compute_slots = [
    {
        "id": "slot1",
        "capacity": "2 GPU",
        "energy_kwh": 1.2,
        "window": "now–+3h",
        "sla": "3h",
        "priority": "low"
    },
    {
        "id": "slot2",
        "capacity": "4 CPU",
        "energy_kwh": 0.6,
        "window": "now–+1h",
        "sla": "1h",
        "priority": "high"
    }
]

@app.route("/on_search", methods=["POST"])
def on_search():
    """Return available compute flexibility slots."""
    return jsonify({"catalog": compute_slots}), 200

@app.route("/on_select", methods=["POST"])
def on_select():
    data = request.json
    slot_id = data.get("slot_id")
    return jsonify({"message": f"Slot {slot_id} selected"}), 200

@app.route("/on_confirm", methods=["POST"])
def on_confirm():
    data = request.json
    slot_id = data.get("slot_id")
    return jsonify({"status": "scheduled", "slot_id": slot_id}), 200

if __name__ == "__main__":
    app.run(port=5001)
