from datetime import datetime
from typing import Dict
from models.job import ComputeJob

class BecknPayloadGenerator:
    """
    Generates Beckn-compliant payloads for different transaction types
    """
    
    @staticmethod
    def generate_search_payload(job: ComputeJob, region: str, 
                               start_time: datetime, end_time: datetime) -> dict:
        """Generate search intent for compute slots"""
        
        return {
            "item": {
                "descriptor": {
                    "name": "compute_slot",
                    "code": "COMPUTE_FLEXIBLE"
                }
            },
            "fulfillment": {
                "type": "SCHEDULED",
                "time": {
                    "range": {
                        "start": start_time.isoformat() + "Z",
                        "end": end_time.isoformat() + "Z"
                    }
                },
                "state": {
                    "descriptor": {
                        "code": "PENDING"
                    }
                }
            },
            "location": {
                "region": region,
                "country": "GB"
            },
            "tags": [
                {
                    "descriptor": {"code": "RESOURCE_REQUIREMENTS"},
                    "list": [
                        {"descriptor": {"code": "cpu_cores"}, "value": str(job.cpu_cores)},
                        {"descriptor": {"code": "gpu_count"}, "value": str(job.gpu_count)},
                        {"descriptor": {"code": "memory_gb"}, "value": str(job.memory_gb)},
                        {"descriptor": {"code": "energy_kwh"}, "value": str(job.energy_kwh)}
                    ]
                },
                {
                    "descriptor": {"code": "JOB_METADATA"},
                    "list": [
                        {"descriptor": {"code": "job_type"}, "value": job.job_type},
                        {"descriptor": {"code": "priority"}, "value": str(job.priority)},
                        {"descriptor": {"code": "can_defer"}, "value": str(job.can_defer)},
                        {"descriptor": {"code": "can_interrupt"}, "value": str(job.can_interrupt)}
                    ]
                }
            ]
        }