import requests

DECISION_URL = "http://127.0.0.1:5003/run_decision"

print("Running Decision Agent...")
resp = requests.get(DECISION_URL).json()
print(resp)
