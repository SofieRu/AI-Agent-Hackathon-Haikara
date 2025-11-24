import os

folders = [
    "compute_agent",
    "grid_agent",
    "decision_agent",
    "audit_agent",
    "api_clients",
    "models",
    "utils",
    "demo"
]

for folder in folders:
    os.makedirs(folder, exist_ok=True)
    print(f"Created folder: {folder}")

print("All folders created successfully!")
