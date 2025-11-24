"""
Orchestrator Agent - Main coordination and decision-making agent
Coordinates all sub-agents and executes the optimization workflow
"""

import asyncio
from datetime import datetime
from typing import List, Dict, Optional
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.compute_monitor import ComputeMonitor
from agents.grid_data_ingestor import GridDataIngestor
from agents.audit_logger import AuditLogger
from optimization.scheduler import FlexibleScheduler
from optimization.forecaster import CarbonAwareForecaster
from beckn.bap_client import BecknOrchestrator
from models.job import ComputeJob, ScheduledJob


class OrchestratorAgent:
    """
    Main orchestration agent that coordinates all sub-agents
    Implements the decision logic and workflow coordination
    
    Responsibilities:
    - Coordinate data ingestion from compute and grid agents
    - Run optimization algorithms
    - Execute schedules via Beckn protocol
    - Log all decisions for auditability
    """
    
    def __init__(self, bap_url: str, subscriber_id: str, private_key: str, 
                 carbon_cap: float, optimization_horizon: int):
        """
        Initialize the orchestrator with all sub-agents
        
        Args:
            bap_url: Beckn BAP sandbox URL
            subscriber_id: Beckn subscriber ID
            private_key: Private key for Beckn signing
            carbon_cap: Maximum carbon intensity (gCO2/kWh)
            optimization_horizon: Planning horizon in hours
        """
        
        print("🚀 Initializing Orchestrator Agent...")
        print(f"   Carbon Cap: {carbon_cap} gCO2/kWh")
        print(f"   Optimization Horizon: {optimization_horizon} hours")
        
        # Initialize sub-agents
        print("   📊 Initializing Compute Monitor...")
        self.compute_monitor = ComputeMonitor()
        
        print("   🌍 Initializing Grid Data Ingestor...")
        self.grid_ingestor = GridDataIngestor()
        
        print("   📝 Initializing Audit Logger...")
        self.audit_logger = AuditLogger()
        
        # Initialize optimization engine
        print("   ⚡ Initializing Scheduler...")
        self.scheduler = FlexibleScheduler(
            carbon_cap=carbon_cap,
            optimization_horizon_hours=optimization_horizon
        )
        
        # Initialize forecaster (with OpenAI if available)
        print("   🤖 Initializing AI Forecaster...")
        openai_key = os.getenv('OPENAI_API_KEY')
        self.forecaster = CarbonAwareForecaster(openai_api_key=openai_key)
        
        # Initialize Beckn orchestrator
        print("   🔗 Initializing Beckn Orchestrator...")
        self.beckn_orchestrator = BecknOrchestrator(
            bap_url=bap_url,
            subscriber_id=subscriber_id,
            private_key=private_key
        )
        
        # Store configuration
        self.carbon_cap = carbon_cap
        self.optimization_horizon = optimization_horizon
        
        # Tracking
        self.cycle_count = 0
        self.total_jobs_processed = 0
        self.total_cost_saved = 0.0
        self.total_carbon_saved = 0.0
        
        print("✅ Orchestrator Agent initialized successfully\n")
    
    async def optimize(self, jobs: List[ComputeJob], grid_forecast: dict) -> List[dict]:
        """
        Main optimization logic that coordinates scheduling decisions
        
        Workflow:
        1. Classify jobs by flexibility
        2. Get AI predictions for optimal windows (if available)
        3. Run MILP optimization
        4. Return optimized schedule
        
        Args:
            jobs: List of compute jobs to schedule
            grid_forecast: Grid conditions forecast (prices, carbon, P415 events)
        
        Returns:
            List of scheduled jobs with timing and region assignments
        """
        
        print(f"\n🧮 Optimizing {len(jobs)} jobs...")
        
        if not jobs:
            print("   ⚠️  No jobs to optimize")
            return []
        
        # Step 1: Classify jobs by flexibility
        immediate_jobs = [j for j in jobs if not j.can_defer or j.priority >= 9]
        flexible_jobs = [j for j in jobs if j.can_defer and j.priority < 9]
        
        print(f"   📌 Immediate jobs (must start now): {len(immediate_jobs)}")
        print(f"   ⏰ Flexible jobs (can be deferred): {len(flexible_jobs)}")
        
        # Step 2: Enrich flexible jobs with AI forecasts
        if flexible_jobs:
            print(f"\n   🤖 Running AI forecasts for {len(flexible_jobs)} flexible jobs...")
            
            for i, job in enumerate(flexible_jobs):
                try:
                    print(f"      [{i+1}/{len(flexible_jobs)}] Forecasting for {job.name} ({job.job_type})...")
                    
                    optimal_windows = await self.forecaster.forecast_optimal_windows(
                        job, 
                        grid_forecast,
                        horizon_hours=self.optimization_horizon
                    )
                    
                    job.preferred_windows = optimal_windows
                    
                    if optimal_windows:
                        best_window = optimal_windows[0]
                        print(f"          ✓ Best window: Hour {best_window['start_hour']}, "
                              f"Region {best_window['region']}, Score {best_window['score']:.1f}/100")
                    else:
                        print(f"          ⚠️  No optimal windows found")
                        
                except Exception as e:
                    print(f"      ⚠️  AI forecast failed for {job.id}: {e}")
                    job.preferred_windows = []
        
        # Step 3: Get datacenter capacity
        print(f"\n   💾 Fetching datacenter capacity...")
        datacenter_capacity = await self.compute_monitor.get_capacity()
        
        for region, capacity in datacenter_capacity.items():
            print(f"      {region}: {capacity['cpu_cores']} CPUs, "
                  f"{capacity['gpu_count']} GPUs available")
        
        # Step 4: Run MILP optimization
        print(f"\n   ⚡ Running MILP optimization...")
        try:
            schedule = self.scheduler.optimize(
                jobs=jobs,
                grid_forecast=grid_forecast,
                datacenter_availability=datacenter_capacity
            )
            
            print(f"   ✅ Optimization complete: {len(schedule)} jobs scheduled\n")
            
            # Print schedule summary
            self._print_schedule_summary(schedule, grid_forecast)
            
            return schedule
            
        except Exception as e:
            print(f"   ❌ Optimization failed: {e}")
            print(f"   🔄 Using fallback FIFO scheduler...")
            import traceback
            traceback.print_exc()
            return self._fallback_schedule(jobs, grid_forecast)
    
    def _print_schedule_summary(self, schedule: List[dict], grid_forecast: dict):
        """Print a summary of the optimized schedule"""
        
        total_cost = sum(s['cost'] for s in schedule)
        total_carbon = sum(s['carbon'] for s in schedule)
        total_p415 = sum(s.get('p415_revenue', 0) for s in schedule)
        total_baseline_cost = sum(s.get('baseline_cost', s['cost']) for s in schedule)
        total_baseline_carbon = sum(s.get('baseline_carbon', s['carbon']) for s in schedule)
        
        cost_savings = total_baseline_cost - total_cost
        carbon_savings = total_baseline_carbon - total_carbon
        net_cost = total_cost - total_p415
        
        print(f"   📊 Schedule Summary:")
        print(f"      💰 Total Cost: £{total_cost:.2f} (savings: £{cost_savings:.2f}, {cost_savings/total_baseline_cost*100:.1f}%)")
        print(f"      🌍 Total Carbon: {total_carbon:.0f} gCO2 (savings: {carbon_savings:.0f} gCO2, {carbon_savings/total_baseline_carbon*100:.1f}%)")
        print(f"      💵 P415 Revenue: £{total_p415:.2f}")
        print(f"      📈 Net Cost: £{net_cost:.2f}")
        
        # Regional breakdown
        region_stats = {}
        for s in schedule:
            region = s['region']
            if region not in region_stats:
                region_stats[region] = {'count': 0, 'energy': 0, 'cost': 0}
            region_stats[region]['count'] += 1
            region_stats[region]['energy'] += s['job'].energy_kwh
            region_stats[region]['cost'] += s['cost']
        
        print(f"\n   📍 Regional Distribution:")
        for region, stats in region_stats.items():
            print(f"      {region}: {stats['count']} jobs, {stats['energy']:.1f} kWh, £{stats['cost']:.2f}")
    
    def _fallback_schedule(self, jobs: List[ComputeJob], grid_forecast: dict) -> List[dict]:
        """
        Simple FIFO fallback schedule if optimization fails
        Schedules all jobs immediately in their preferred regions
        
        Args:
            jobs: List of jobs to schedule
            grid_forecast: Grid forecast data
        
        Returns:
            Basic schedule with immediate execution
        """
        
        schedule = []
        
        for job in jobs:
            # Use preferred region or default to 'south'
            region = job.preferred_region or 'south'
            
            # Use current hour pricing (index 0)
            prices = grid_forecast['price']
            carbon = grid_forecast['carbon']
            
            current_price = prices[region][0] if region in prices and len(prices[region]) > 0 else 0.15
            current_carbon = carbon[region][0] if region in carbon and len(carbon[region]) > 0 else 200
            
            schedule.append({
                'job': job,
                'start_time': 0,  # Start immediately
                'start_datetime': datetime.now(),
                'end_datetime': datetime.now(),
                'region': region,
                'cost': job.energy_kwh * current_price,
                'carbon': job.energy_kwh * current_carbon,
                'p415_revenue': 0,
                'baseline_cost': job.energy_kwh * 0.15,
                'baseline_carbon': job.energy_kwh * 250
            })
        
        print(f"   ✅ Fallback schedule created: {len(schedule)} jobs")
        return schedule
    
    async def execute_schedule(self, schedule: List[dict]):
        """
        Execute the optimized schedule via Beckn protocol
        
        Args:
            schedule: List of scheduled jobs to execute
        """
        
        print(f"\n🔄 Executing schedule via Beckn protocol...")
        
        if not schedule:
            print("   ⚠️  No jobs to execute")
            return
        
        try:
            await self.beckn_orchestrator.execute_schedule(schedule)
            print(f"   ✅ Schedule execution initiated for {len(schedule)} jobs")
            
            # Update job statuses
            for scheduled in schedule:
                self.compute_monitor.update_job_status(
                    scheduled['job'].id, 
                    'scheduled'
                )
            
        except Exception as e:
            print(f"   ❌ Schedule execution failed: {e}")
            import traceback
            traceback.print_exc()
    
    async def run_cycle(self):
        """
        Main orchestration cycle - the heart of the system
        
        Workflow:
        1. Fetch pending compute jobs
        2. Fetch grid forecast data
        3. Run optimization
        4. Execute via Beckn
        5. Log everything for audit
        """
        
        self.cycle_count += 1
        
        print("\n" + "="*80)
        print(f"🔄 ORCHESTRATION CYCLE #{self.cycle_count}")
        print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        try:
            # Step 1: Ingest new jobs
            print("\n📥 STEP 1: Fetching pending compute jobs...")
            new_jobs = await self.compute_monitor.get_pending_jobs()
            print(f"   Found {len(new_jobs)} pending jobs")
            
            if new_jobs:
                for i, job in enumerate(new_jobs):
                    print(f"   [{i+1}] {job.name} ({job.job_type}) - "
                          f"Priority: {job.priority}, Energy: {job.energy_kwh:.1f} kWh, "
                          f"Flexible: {job.can_defer}")
            else:
                print("   ℹ️  No jobs in queue")
                print("\n⏭️  Skipping cycle (no work to do)\n")
                return
            
            # Step 2: Fetch grid data
            print("\n🌍 STEP 2: Fetching grid forecast data...")
            grid_forecast = await self.grid_ingestor.fetch_forecast(
                horizon_hours=self.optimization_horizon
            )
            
            print(f"   ✓ Grid data retrieved:")
            print(f"      Average Carbon: {grid_forecast['avg_carbon']:.2f} gCO2/kWh")
            print(f"      Average Price: £{grid_forecast['avg_price']:.4f}/kWh")
            print(f"      P415 Events: {len(grid_forecast['p415_events'])} flexibility opportunities")
            
            # Show best regions
            carbon_by_region = {
                region: sum(values) / len(values) 
                for region, values in grid_forecast['carbon'].items()
            }
            best_carbon_region = min(carbon_by_region, key=carbon_by_region.get)
            print(f"      Lowest Carbon Region: {best_carbon_region} "
                  f"({carbon_by_region[best_carbon_region]:.1f} gCO2/kWh)")
            
            # Step 3: Run optimization
            print("\n⚡ STEP 3: Running optimization engine...")
            optimized_schedule = await self.optimize(
                jobs=new_jobs,
                grid_forecast=grid_forecast
            )
            
            if not optimized_schedule:
                print("   ⚠️  No schedule generated")
                return
            
            # Step 4: Execute via Beckn
            print("\n🔗 STEP 4: Executing schedule via Beckn protocol...")
            await self.execute_schedule(optimized_schedule)
            
            # Step 5: Log to audit trail
            print("\n📝 STEP 5: Logging to audit trail...")
            await self.audit_logger.log_decision(
                jobs=new_jobs,
                schedule=optimized_schedule,
                grid_state=grid_forecast
            )
            
            # Update metrics
            self.total_jobs_processed += len(optimized_schedule)
            cost_saved = sum(s.get('baseline_cost', 0) - s['cost'] for s in optimized_schedule)
            carbon_saved = sum(s.get('baseline_carbon', 0) - s['carbon'] for s in optimized_schedule)
            self.total_cost_saved += cost_saved
            self.total_carbon_saved += carbon_saved
            
            print(f"\n   ✓ Audit log written")
            print(f"   ✓ Cycle complete!")
            
            # Print cumulative statistics
            print(f"\n📊 CUMULATIVE STATISTICS:")
            print(f"   Total Cycles: {self.cycle_count}")
            print(f"   Total Jobs Processed: {self.total_jobs_processed}")
            print(f"   Total Cost Saved: £{self.total_cost_saved:.2f}")
            print(f"   Total Carbon Saved: {self.total_carbon_saved/1000:.2f} kg CO2")
            print(f"   Average Savings per Job: £{self.total_cost_saved/max(1, self.total_jobs_processed):.2f}")
            
        except Exception as e:
            print(f"\n❌ CYCLE FAILED: {e}")
            import traceback
            traceback.print_exc()
        
        print("\n" + "="*80 + "\n")
    
    def get_current_schedule(self) -> List[dict]:
        """
        Get currently scheduled jobs
        
        Returns:
            List of scheduled job details
        """
        return [
            {
                'job': job,
                'status': job.status,
                'submitted_at': job.submitted_at.isoformat(),
                'deadline': job.deadline.isoformat()
            }
            for job in self.compute_monitor.scheduled_jobs
        ]
    
    def get_metrics(self) -> dict:
        """
        Get current system metrics
        
        Returns:
            Dictionary of performance metrics
        """
        return {
            'cycle_count': self.cycle_count,
            'total_jobs_processed': self.total_jobs_processed,
            'total_cost_saved': self.total_cost_saved,
            'total_carbon_saved_kg': self.total_carbon_saved / 1000,
            'pending_jobs': len(self.compute_monitor.pending_jobs),
            'scheduled_jobs': len(self.compute_monitor.scheduled_jobs),
            'running_jobs': len(self.compute_monitor.running_jobs),
            'completed_jobs': len(self.compute_monitor.completed_jobs),
            'p415_revenue': self.audit_logger.get_p415_revenue()
        }
    
    async def shutdown(self):
        """Graceful shutdown procedure"""
        print("\n🛑 Initiating graceful shutdown...")
        
        # Generate final audit report
        print("   📊 Generating final audit report...")
        audit_report = await self.audit_logger.generate_audit_report()
        
        print(f"\n   Final Statistics:")
        print(f"      Cycles Run: {self.cycle_count}")
        print(f"      Jobs Processed: {self.total_jobs_processed}")
        print(f"      Total Savings: £{self.total_cost_saved:.2f}")
        print(f"      Carbon Saved: {self.total_carbon_saved/1000:.2f} kg CO2")
        
        print("\n   ✅ Shutdown complete")


if __name__ == "__main__":
    """Test the orchestrator independently"""
    
    async def test():
        orchestrator = OrchestratorAgent(
            bap_url="https://bap-sandbox.beckn.org",
            subscriber_id="test-orchestrator",
            private_key="",
            carbon_cap=100,
            optimization_horizon=24
        )
        
        # Run one test cycle
        await orchestrator.run_cycle()
        
        # Print metrics
        metrics = orchestrator.get_metrics()
        print("\n📊 Test Metrics:")
        for key, value in metrics.items():
            print(f"   {key}: {value}")
    
    asyncio.run(test())