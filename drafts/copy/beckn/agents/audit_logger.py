"""
Audit Logger Agent - Comprehensive logging for compliance and auditability
Logs all decisions, Beckn transactions, and system metrics
"""

import json
from datetime import datetime
from typing import List, Dict, Optional
import hashlib
from pathlib import Path
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class AuditLogger:
    """
    Logs all decisions, Beckn transactions, and outcomes for full auditability
    
    In production, this would:
    - Write to blockchain for immutability
    - Store in distributed ledger (e.g., Hyperledger)
    - Integrate with compliance systems
    - Generate regulatory reports
    
    For demo: Writes to local JSON logs with cryptographic hashing
    """
    
    def __init__(self, log_dir: str = "./logs"):
        """
        Initialize audit logger
        
        Args:
            log_dir: Directory to store log files
        """
        
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Create separate log files for different types of records
        self.decision_log = self.log_dir / "decisions.jsonl"
        self.beckn_log = self.log_dir / "beckn_transactions.jsonl"
        self.metrics_log = self.log_dir / "metrics.jsonl"
        self.audit_trail = self.log_dir / "audit_trail.jsonl"
        
        # In-memory metrics for quick access
        self.total_cost_saved = 0.0
        self.total_carbon_saved = 0.0
        self.total_p415_revenue = 0.0
        self.jobs_processed = 0
        self.decisions_logged = 0
        self.beckn_transactions_logged = 0
        
        print(f"✅ Audit Logger initialized (logs: {self.log_dir})")
    
    async def log_decision(self, jobs: List, schedule: List[dict], grid_state: dict):
        """
        Log an optimization decision
        
        Records:
        - Input jobs and their characteristics
        - Grid state at time of decision
        - Resulting schedule
        - Cost and carbon metrics
        - Cryptographic hash for integrity
        
        Args:
            jobs: List of input jobs
            schedule: Optimized schedule
            grid_state: Grid forecast data used
        """
        
        # Calculate metrics
        total_cost = sum(s.get('cost', 0) for s in schedule)
        total_carbon = sum(s.get('carbon', 0) for s in schedule)
        total_p415 = sum(s.get('p415_revenue', 0) for s in schedule)
        total_baseline_cost = sum(s.get('baseline_cost', s.get('cost', 0)) for s in schedule)
        total_baseline_carbon = sum(s.get('baseline_carbon', s.get('carbon', 0)) for s in schedule)
        
        cost_savings = total_baseline_cost - total_cost
        carbon_savings = total_baseline_carbon - total_carbon
        
        # Build decision entry
        decision_entry = {
            'timestamp': datetime.now().isoformat(),
            'decision_id': f"decision_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.decisions_logged}",
            'jobs_count': len(jobs),
            'scheduled_count': len(schedule),
            
            # Grid state summary
            'grid_state': {
                'avg_price': grid_state.get('avg_price', 0),
                'avg_carbon': grid_state.get('avg_carbon', 0),
                'p415_events_count': len(grid_state.get('p415_events', [])),
                'timestamp': grid_state.get('timestamp', '')
            },
            
            # Input jobs summary
            'jobs_summary': [
                {
                    'id': j.id,
                    'name': j.name,
                    'type': j.job_type,
                    'priority': j.priority,
                    'energy_kwh': j.energy_kwh,
                    'can_defer': j.can_defer,
                    'deadline': j.deadline.isoformat()
                }
                for j in jobs
            ],
            
            # Schedule output
            'schedule': [
                {
                    'job_id': s['job'].id,
                    'job_name': s['job'].name,
                    'job_type': s['job'].job_type,
                    'start_time': s.get('start_time', 0),
                    'start_datetime': s.get('start_datetime', datetime.now()).isoformat() if hasattr(s.get('start_datetime', datetime.now()), 'isoformat') else str(s.get('start_datetime')),
                    'region': s['region'],
                    'cost': s['cost'],
                    'carbon': s['carbon'],
                    'p415_revenue': s.get('p415_revenue', 0),
                    'baseline_cost': s.get('baseline_cost', s['cost']),
                    'baseline_carbon': s.get('baseline_carbon', s['carbon'])
                }
                for s in schedule
            ],
            
            # Metrics
            'metrics': {
                'total_cost': total_cost,
                'total_carbon': total_carbon,
                'total_p415_revenue': total_p415,
                'cost_savings': cost_savings,
                'carbon_savings': carbon_savings,
                'net_cost': total_cost - total_p415,
                'cost_savings_percent': (cost_savings / total_baseline_cost * 100) if total_baseline_cost > 0 else 0,
                'carbon_savings_percent': (carbon_savings / total_baseline_carbon * 100) if total_baseline_carbon > 0 else 0
            },
            
            # Regional distribution
            'regional_distribution': self._calculate_regional_distribution(schedule)
        }
        
        # Add cryptographic hash for integrity
        decision_entry['hash'] = self._calculate_hash(decision_entry)
        
        # Write to log file (append mode, JSONL format)
        with open(self.decision_log, 'a') as f:
            f.write(json.dumps(decision_entry) + '\n')
        
        # Update in-memory metrics
        self.total_cost_saved += cost_savings
        self.total_carbon_saved += carbon_savings
        self.total_p415_revenue += total_p415
        self.jobs_processed += len(schedule)
        self.decisions_logged += 1
        
        print(f"   ✓ Decision logged: {decision_entry['decision_id']}")
        print(f"      Cost savings: £{cost_savings:.2f} ({cost_savings/total_baseline_cost*100:.1f}%)")
        print(f"      Carbon savings: {carbon_savings:.0f} gCO2 ({carbon_savings/total_baseline_carbon*100:.1f}%)")
    
    async def log_beckn_transaction(self, transaction_type: str, payload: dict, response: dict):
        """
        Log Beckn protocol transaction
        
        Records complete transaction details for regulatory compliance
        
        Args:
            transaction_type: Type of Beckn transaction (search, select, confirm, etc.)
            payload: Request payload sent
            response: Response received
        """
        
        transaction_entry = {
            'timestamp': datetime.now().isoformat(),
            'transaction_type': transaction_type,
            'transaction_id': payload.get('context', {}).get('transaction_id', 'unknown'),
            'message_id': payload.get('context', {}).get('message_id', 'unknown'),
            'bap_id': payload.get('context', {}).get('bap_id', ''),
            'payload': payload,
            'response': response,
            'signature': self._calculate_hash(payload)
        }
        
        # Write to Beckn log
        with open(self.beckn_log, 'a') as f:
            f.write(json.dumps(transaction_entry) + '\n')
        
        self.beckn_transactions_logged += 1
        
        print(f"   📝 Beckn transaction logged: {transaction_type} ({transaction_entry['transaction_id'][:8]}...)")
    
    async def log_metrics(self, metrics: dict):
        """
        Log system performance metrics
        
        Args:
            metrics: Dictionary of metrics to log
        """
        
        metrics_entry = {
            'timestamp': datetime.now().isoformat(),
            **metrics
        }
        
        with open(self.metrics_log, 'a') as f:
            f.write(json.dumps(metrics_entry) + '\n')
    
    async def log_audit_event(self, event_type: str, description: str, metadata: dict = None):
        """
        Log general audit event
        
        Args:
            event_type: Type of event (e.g., 'system_start', 'error', 'warning')
            description: Human-readable description
            metadata: Additional event metadata
        """
        
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'description': description,
            'metadata': metadata or {}
        }
        
        with open(self.audit_trail, 'a') as f:
            f.write(json.dumps(audit_entry) + '\n')
    
    def _calculate_regional_distribution(self, schedule: List[dict]) -> dict:
        """Calculate job distribution across regions"""
        
        distribution = {}
        
        for s in schedule:
            region = s['region']
            if region not in distribution:
                distribution[region] = {
                    'count': 0,
                    'total_energy_kwh': 0,
                    'total_cost': 0,
                    'total_carbon': 0,
                    'p415_revenue': 0
                }
            
            distribution[region]['count'] += 1
            distribution[region]['total_energy_kwh'] += s['job'].energy_kwh
            distribution[region]['total_cost'] += s['cost']
            distribution[region]['total_carbon'] += s['carbon']
            distribution[region]['p415_revenue'] += s.get('p415_revenue', 0)
        
        return distribution
    
    def _calculate_hash(self, data) -> str:
        """
        Calculate SHA-256 hash for data integrity
        
        Args:
            data: Data to hash (will be JSON serialized)
        
        Returns:
            Hex string of SHA-256 hash
        """
        
        # Serialize to JSON with sorted keys for consistency
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def get_total_savings(self) -> float:
        """Get total cost savings"""
        return self.total_cost_saved
    
    def get_carbon_savings(self) -> float:
        """Get total carbon savings in tons"""
        return self.total_carbon_saved / 1000000  # Convert gCO2 to tons
    
    def get_p415_revenue(self) -> float:
        """Get total P415 revenue"""
        return self.total_p415_revenue
    
    def get_jobs_processed(self) -> int:
        """Get total jobs processed"""
        return self.jobs_processed
    
    async def generate_audit_report(self) -> dict:
        """
        Generate comprehensive audit report
        
        Returns:
            Dictionary with complete audit statistics
        """
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'decisions_logged': self.decisions_logged,
                'beckn_transactions_logged': self.beckn_transactions_logged,
                'jobs_processed': self.jobs_processed,
                'total_cost_saved': self.total_cost_saved,
                'total_carbon_saved_tons': self.get_carbon_savings(),
                'total_p415_revenue': self.total_p415_revenue,
                'net_savings': self.total_cost_saved + self.total_p415_revenue,
                'average_savings_per_job': self.total_cost_saved / max(1, self.jobs_processed)
            },
            'log_files': {
                'decisions': str(self.decision_log),
                'beckn_transactions': str(self.beckn_log),
                'metrics': str(self.metrics_log),
                'audit_trail': str(self.audit_trail)
            },
            'integrity': {
                'decisions_hash': self._hash_log_file(self.decision_log),
                'beckn_hash': self._hash_log_file(self.beckn_log)
            }
        }
        
        return report
    
    def _hash_log_file(self, filepath: Path) -> str:
        """Calculate hash of entire log file for integrity check"""
        
        if not filepath.exists():
            return "file_not_found"
        
        hasher = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        
        return hasher.hexdigest()
    
    def get_recent_decisions(self, count: int = 10) -> List[dict]:
        """
        Get most recent decisions
        
        Args:
            count: Number of recent decisions to retrieve
        
        Returns:
            List of decision entries
        """
        
        if not self.decision_log.exists():
            return []
        
        with open(self.decision_log, 'r') as f:
            lines = f.readlines()
        
        # Get last N lines
        recent_lines = lines[-count:]
        
        decisions = []
        for line in recent_lines:
            try:
                decisions.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        
        return decisions
    
    def get_recent_beckn_transactions(self, count: int = 10) -> List[dict]:
        """Get most recent Beckn transactions"""
        
        if not self.beckn_log.exists():
            return []
        
        with open(self.beckn_log, 'r') as f:
            lines = f.readlines()
        
        recent_lines = lines[-count:]
        
        transactions = []
        for line in recent_lines:
            try:
                transactions.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        
        return transactions


if __name__ == "__main__":
    """Test the audit logger independently"""
    
    async def test():
        logger = AuditLogger(log_dir="./test_logs")
        
        print("\n=== Testing Audit Logger ===\n")
        
        # Mock data for testing
        from models.job import ComputeJob
        from datetime import datetime, timedelta
        
        # Create mock job
        job = ComputeJob(
            id="test_job_001",
            name="Test ML Training",
            job_type="training",
            priority=7,
            cpu_cores=100,
            gpu_count=4,
            memory_gb=256,
            energy_kwh=150,
            power_mw=0.05,
            duration_hours=3,
            submitted_at=datetime.now(),
            earliest_start=datetime.now(),
            deadline=datetime.now() + timedelta(hours=12),
            can_defer=True
        )
        
        # Mock schedule
        schedule = [
            {
                'job': job,
                'start_time': 2,
                'start_datetime': datetime.now() + timedelta(hours=2),
                'region': 'scotland',
                'cost': 15.50,
                'carbon': 12000,
                'p415_revenue': 5.00,
                'baseline_cost': 22.50,
                'baseline_carbon': 18000
            }
        ]
        
        # Mock grid state
        grid_state = {
            'avg_price': 0.15,
            'avg_carbon': 200,
            'p415_events': [],
            'timestamp': datetime.now().isoformat()
        }
        
        # Test decision logging
        print("1. Logging decision...")
        await logger.log_decision([job], schedule, grid_state)
        
        # Test metrics
        print("\n2. Current metrics:")
        print(f"   Total savings: £{logger.get_total_savings():.2f}")
        print(f"   Carbon saved: {logger.get_carbon_savings():.4f} tons")
        print(f"   Jobs processed: {logger.get_jobs_processed()}")
        
        # Test audit report
        print("\n3. Generating audit report...")
        report = await logger.generate_audit_report()
        print(f"   Report generated: {json.dumps(report['summary'], indent=2)}")
        
        # Test recent decisions
        print("\n4. Recent decisions:")
        recent = logger.get_recent_decisions(5)
        print(f"   Found {len(recent)} recent decisions")
    
    import asyncio
    asyncio.run(test())