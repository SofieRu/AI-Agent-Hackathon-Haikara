from flask import Flask, request, jsonify
import requests

app = Flask(__name__)
COMPUTE_AGENT_URL = "http://127.0.0.1:5001"

@app.route("/search", methods=["POST"])
def search():
    """Call Compute Agent's /on_search."""
    resp = requests.post(f"{COMPUTE_AGENT_URL}/on_search")
    slots = resp.json().get("catalog", [])
    return jsonify({"slots": slots}), 200

@app.route("/select", methods=["POST"])
def select():
    data = request.json
    slot_id = data["slot_id"]
    resp = requests.post(f"{COMPUTE_AGENT_URL}/on_select", json={"slot_id": slot_id})
    return jsonify(resp.json()), 200

@app.route("/confirm", methods=["POST"])
def confirm():
    data = request.json
    slot_id = data["slot_id"]
    resp = requests.post(f"{COMPUTE_AGENT_URL}/on_confirm", json={"slot_id": slot_id})
    return jsonify(resp.json()), 200

if __name__ == "__main__":
    app.run(port=5002)
