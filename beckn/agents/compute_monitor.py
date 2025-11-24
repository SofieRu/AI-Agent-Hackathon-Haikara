"""
Compute Monitor Agent - Tracks compute workloads and datacenter capacity
Simulates job queue and resource availability
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Dict
import random
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.job import ComputeJob


class ComputeMonitor:
    """
    Monitors compute workloads and datacenter capacity
    
    In production, this would integrate with:
    - Kubernetes for container orchestration
    - Slurm for HPC job scheduling
    - Cloud provider APIs (AWS, Azure, GCP)
    
    For demo, generates realistic mock jobs
    """
    
    def __init__(self):
        """Initialize compute monitor with empty queues"""
        
        # Job queues
        self.pending_jobs = []
        self.scheduled_jobs = []
        self.running_jobs = []
        self.completed_jobs = []
        self.failed_jobs = []
        
        # Datacenter capacity (mock data for 3 regions)
        self.datacenter_capacity = {
            'north': {
                'name': 'North England DC',
                'cpu_cores': 10000,
                'gpu_count': 500,
                'memory_gb': 100000,
                'max_power_mw': 50,
                'available_cpu': 10000,
                'available_gpu': 500,
                'available_memory_gb': 100000,
                'utilization_percent': 0
            },
            'south': {
                'name': 'South England DC',
                'cpu_cores': 15000,
                'gpu_count': 800,
                'memory_gb': 150000,
                'max_power_mw': 75,
                'available_cpu': 15000,
                'available_gpu': 800,
                'available_memory_gb': 150000,
                'utilization_percent': 0
            },
            'scotland': {
                'name': 'Scotland DC (High Renewables)',
                'cpu_cores': 8000,
                'gpu_count': 400,
                'memory_gb': 80000,
                'max_power_mw': 40,
                'available_cpu': 8000,
                'available_gpu': 400,
                'available_memory_gb': 80000,
                'utilization_percent': 0
            }
        }
        
        # Job generation parameters
        self.job_counter = 0
        self.last_job_generation = datetime.now()
        
        print("✅ Compute Monitor initialized")
    
    async def get_pending_jobs(self) -> List[ComputeJob]:
        """
        Fetch pending compute jobs from queue
        
        In production, this would:
        - Query Kubernetes pod queue
        - Check Slurm job scheduler
        - Poll cloud provider job queues
        
        For demo: Generates realistic mock jobs with varying characteristics
        
        Returns:
            List of pending compute jobs
        """
        
        # Simulate job arrivals (30% chance per call)
        if random.random() > 0.3:
            new_jobs = self._generate_mock_jobs(random.randint(1, 5))
            self.pending_jobs.extend(new_jobs)
        
        return self.pending_jobs.copy()
    
    def _generate_mock_jobs(self, count: int) -> List[ComputeJob]:
        """
        Generate realistic mock compute jobs
        
        Job types:
        - Training: Long-running ML model training (high energy, flexible)
        - Batch Inference: ML inference on data batches (medium energy, some flexibility)
        - Analytics: Data processing/analytics (low-medium energy, very flexible)
        
        Args:
            count: Number of jobs to generate
        
        Returns:
            List of generated compute jobs
        """
        
        jobs = []
        job_types = ['training', 'batch_inference', 'analytics', 'data_processing', 'simulation']
        
        for i in range(count):
            self.job_counter += 1
            now = datetime.now()
            job_type = random.choice(job_types)
            
            # Job characteristics vary by type
            if job_type == 'training':
                # ML Training: Long duration, high energy, very flexible
                duration = random.uniform(2, 8)  # 2-8 hours
                energy = random.uniform(100, 300)  # 100-300 kWh
                priority = random.randint(5, 8)
                can_defer = True
                can_interrupt = random.choice([True, False])  # Some training can checkpoint
                flexibility = random.randint(240, 720)  # 4-12 hours flexibility
                cpu_cores = random.randint(100, 500)
                gpu_count = random.randint(4, 16)
                memory_gb = random.uniform(128, 512)
                
            elif job_type == 'batch_inference':
                # Batch Inference: Medium duration, medium energy, some flexibility
                duration = random.uniform(0.5, 3)  # 30 min - 3 hours
                energy = random.uniform(20, 100)  # 20-100 kWh
                priority = random.randint(6, 9)
                can_defer = True
                can_interrupt = False
                flexibility = random.randint(60, 240)  # 1-4 hours flexibility
                cpu_cores = random.randint(50, 200)
                gpu_count = random.randint(1, 8)
                memory_gb = random.uniform(64, 256)
                
            elif job_type == 'analytics':
                # Analytics: Variable duration, flexible
                duration = random.uniform(1, 6)  # 1-6 hours
                energy = random.uniform(30, 150)  # 30-150 kWh
                priority = random.randint(3, 7)
                can_defer = True
                can_interrupt = True
                flexibility = random.randint(360, 1440)  # 6-24 hours flexibility
                cpu_cores = random.randint(50, 300)
                gpu_count = random.randint(0, 4)
                memory_gb = random.uniform(32, 256)
                
            elif job_type == 'data_processing':
                # Data Processing: Medium duration, flexible
                duration = random.uniform(0.5, 4)  # 30 min - 4 hours
                energy = random.uniform(10, 80)  # 10-80 kWh
                priority = random.randint(4, 7)
                can_defer = True
                can_interrupt = True
                flexibility = random.randint(120, 480)  # 2-8 hours flexibility
                cpu_cores = random.randint(20, 200)
                gpu_count = 0
                memory_gb = random.uniform(64, 256)
                
            else:  # simulation
                # Simulation: Long duration, very flexible
                duration = random.uniform(3, 12)  # 3-12 hours
                energy = random.uniform(50, 200)  # 50-200 kWh
                priority = random.randint(3, 6)
                can_defer = True
                can_interrupt = True
                flexibility = random.randint(480, 1440)  # 8-24 hours flexibility
                cpu_cores = random.randint(100, 400)
                gpu_count = random.randint(0, 8)
                memory_gb = random.uniform(128, 512)
            
            # Calculate power draw
            power_mw = energy / duration / 1000  # Convert to MW
            
            # Set deadline based on flexibility
            deadline = now + timedelta(minutes=flexibility + random.randint(60, 240))
            
            # Determine SLA type
            if priority >= 9:
                sla_type = 'critical'
            elif priority >= 6:
                sla_type = 'standard'
            else:
                sla_type = 'flexible'
            
            # Create job
            job = ComputeJob(
                id=f"job_{self.job_counter:05d}_{datetime.now().strftime('%H%M%S')}",
                name=f"{job_type.replace('_', ' ').title()} #{self.job_counter}",
                job_type=job_type,
                priority=priority,
                cpu_cores=cpu_cores,
                gpu_count=gpu_count,
                memory_gb=memory_gb,
                energy_kwh=energy,
                power_mw=power_mw,
                duration_hours=duration,
                submitted_at=now,
                earliest_start=now,
                deadline=deadline,
                can_defer=can_defer,
                can_interrupt=can_interrupt,
                can_migrate=True,
                flexibility_minutes=flexibility,
                preferred_region=random.choice(['north', 'south', 'scotland', None]),
                sla_type=sla_type,
                status='pending'
            )
            
            jobs.append(job)
        
        return jobs
    
    async def get_capacity(self) -> Dict[str, dict]:
        """
        Get current datacenter capacity across all regions
        
        In production, this would:
        - Query Kubernetes node resources
        - Check cloud provider quotas
        - Monitor actual hardware utilization
        
        Returns:
            Dictionary mapping region -> capacity metrics
        """
        
        # Simulate some capacity changes (jobs running, completing)
        for region in self.datacenter_capacity:
            # Random utilization between 0-50%
            utilization = random.uniform(0, 0.5)
            self.datacenter_capacity[region]['utilization_percent'] = utilization * 100
            
            # Update available resources
            total_cpu = self.datacenter_capacity[region]['cpu_cores']
            total_gpu = self.datacenter_capacity[region]['gpu_count']
            total_memory = self.datacenter_capacity[region]['memory_gb']
            
            self.datacenter_capacity[region]['available_cpu'] = int(total_cpu * (1 - utilization))
            self.datacenter_capacity[region]['available_gpu'] = int(total_gpu * (1 - utilization))
            self.datacenter_capacity[region]['available_memory_gb'] = total_memory * (1 - utilization)
        
        return self.datacenter_capacity.copy()
    
    def update_job_status(self, job_id: str, status: str):
        """
        Update job status and move between queues
        
        Args:
            job_id: Job identifier
            status: New status ('scheduled', 'running', 'completed', 'failed')
        """
        
        # Find job in pending queue
        job = None
        for j in self.pending_jobs:
            if j.id == job_id:
                job = j
                break
        
        if not job:
            # Check other queues
            for j in self.scheduled_jobs + self.running_jobs:
                if j.id == job_id:
                    job = j
                    break
        
        if job:
            old_status = job.status
            job.status = status
            
            # Move between queues
            if status == "scheduled" and old_status == "pending":
                self.pending_jobs.remove(job)
                self.scheduled_jobs.append(job)
            
            elif status == "running" and old_status == "scheduled":
                self.scheduled_jobs.remove(job)
                self.running_jobs.append(job)
            
            elif status == "completed" and old_status == "running":
                self.running_jobs.remove(job)
                self.completed_jobs.append(job)
            
            elif status == "failed":
                if job in self.pending_jobs:
                    self.pending_jobs.remove(job)
                elif job in self.scheduled_jobs:
                    self.scheduled_jobs.remove(job)
                elif job in self.running_jobs:
                    self.running_jobs.remove(job)
                self.failed_jobs.append(job)
    
    async def get_job_count(self) -> Dict[str, int]:
        """
        Get job counts by status
        
        Returns:
            Dictionary with counts for each status
        """
        return {
            'pending': len(self.pending_jobs),
            'scheduled': len(self.scheduled_jobs),
            'running': len(self.running_jobs),
            'completed': len(self.completed_jobs),
            'failed': len(self.failed_jobs),
            'total': len(self.pending_jobs) + len(self.scheduled_jobs) + 
                    len(self.running_jobs) + len(self.completed_jobs) + len(self.failed_jobs)
        }
    
    def get_job_by_id(self, job_id: str) -> ComputeJob:
        """
        Retrieve specific job by ID
        
        Args:
            job_id: Job identifier
        
        Returns:
            ComputeJob object or None
        """
        all_jobs = (self.pending_jobs + self.scheduled_jobs + 
                   self.running_jobs + self.completed_jobs + self.failed_jobs)
        
        for job in all_jobs:
            if job.id == job_id:
                return job
        
        return None
    
    async def simulate_job_completion(self):
        """
        Simulate jobs completing (for testing)
        Moves random running jobs to completed status
        """
        if self.running_jobs and random.random() > 0.7:
            job = random.choice(self.running_jobs)
            self.update_job_status(job.id, 'completed')
            print(f"   ✓ Job {job.id} completed")


if __name__ == "__main__":
    """Test the compute monitor independently"""
    
    async def test():
        monitor = ComputeMonitor()
        
        print("\n=== Testing Compute Monitor ===\n")
        
        # Test job generation
        print("1. Generating jobs...")
        jobs = await monitor.get_pending_jobs()
        print(f"   Generated {len(jobs)} jobs")
        
        for job in jobs:
            print(f"   - {job.name}: {job.energy_kwh:.1f} kWh, Priority {job.priority}, "
                  f"Flexible: {job.can_defer}")
        
        # Test capacity
        print("\n2. Checking capacity...")
        capacity = await monitor.get_capacity()
        for region, cap in capacity.items():
            print(f"   {region}: {cap['available_cpu']}/{cap['cpu_cores']} CPUs available "
                  f"({cap['utilization_percent']:.1f}% utilized)")
        
        # Test status updates
        print("\n3. Testing status updates...")
        if jobs:
            test_job = jobs[0]
            print(f"   Job {test_job.id}: {test_job.status}")
            monitor.update_job_status(test_job.id, 'scheduled')
            print(f"   Job {test_job.id}: {test_job.status}")
        
        # Test job counts
        print("\n4. Job counts:")
        counts = await monitor.get_job_count()
        for status, count in counts.items():
            print(f"   {status}: {count}")
    
    asyncio.run(test())