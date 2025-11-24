"""
Agents module for Flexible Compute Scheduler
Contains all agent implementations for orchestration, monitoring, and logging
"""

from .orchestrator import OrchestratorAgent
from .compute_monitor import ComputeMonitor
from .grid_data_ingestor import GridDataIngestor
from .audit_logger import AuditLogger

__all__ = [
    'OrchestratorAgent',
    'ComputeMonitor',
    'GridDataIngestor',
    'AuditLogger'
]

__version__ = '1.0.0'