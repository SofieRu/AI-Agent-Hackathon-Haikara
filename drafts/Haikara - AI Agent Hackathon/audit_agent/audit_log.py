from flask import Flask, request, jsonify
import time

app = Flask(__name__)
logs = []

@app.route("/log", methods=["POST"])
def log():
    entry = request.json
    entry["timestamp"] = time.time()
    logs.append(entry)
    return jsonify({"status": "logged"}), 200

@app.route("/logs", methods=["GET"])
def get_logs():
    return jsonify(logs), 200

if __name__ == "__main__":
    app.run(port=5004)
