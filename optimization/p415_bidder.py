from typing import Dict, List
from models.job import ComputeJob
from datetime import datetime

class P415BidEngine:
    """
    Manages participation in P415 flexibility markets
    Calculates bid values and manages market interactions
    """
    
    def __init__(self, revenue_share: float = 0.10):
        self.revenue_share = revenue_share
        self.active_bids = []
        
        # P415 product specifications
        self.products = {
            'DC': {  # Dynamic Containment
                'name': 'Dynamic Containment',
                'response_time_seconds': 1,
                'min_duration_minutes': 30,
                'typical_price_range': (50, 200)  # £/MW/h
            },
            'DM': {  # Dynamic Moderation
                'name': 'Dynamic Moderation',
                'response_time_seconds': 300,
                'min_duration_minutes': 60,
                'typical_price_range': (30, 150)
            },
            'DR': {  # Dynamic Regulation
                'name': 'Dynamic Regulation',
                'response_time_seconds': 600,
                'min_duration_minutes': 120,
                'typical_price_range': (20, 100)
            }
        }
    
    def calculate_flexibility_value(self, job: ComputeJob, time_window: int, 
                                    grid_event: dict) -> float:
        """
        Calculate potential revenue from P415 participation
        
        Args:
            job: The compute job
            time_window: Hour of execution
            grid_event: P415 market event details
        
        Returns:
            Expected revenue in £
        """
        
        product = grid_event.get('product')
        clearing_price = grid_event.get('clearing_price', 0)
        duration_minutes = grid_event.get('duration_minutes', 30)
        
        if product not in self.products:
            return 0.0
        
        product_spec = self.products[product]
        
        # Check if job can participate
        if product == 'DC':
            # Requires immediate interrupt capability
            if not job.can_interrupt:
                return 0.0
            revenue_multiplier = 0.8
            
        elif product == 'DM':
            # Requires 5-minute deferral
            if not job.can_defer or job.flexibility_minutes < 5:
                return 0.0
            revenue_multiplier = 0.5
            
        elif product == 'DR':
            # Slower response acceptable
            if not job.can_defer:
                return 0.0
            revenue_multiplier = 0.3
        else:
            return 0.0
        
        # Calculate revenue
        # Revenue = Clearing Price (£/MW/h) × Power (MW) × Duration (h) × Multiplier
        duration_hours = duration_minutes / 60
        revenue = clearing_price * job.power_mw * duration_hours * revenue_multiplier
        
        # Platform takes a share
        platform_revenue = revenue * self.revenue_share
        
        return revenue - platform_revenue
    
    def create_bid(self, job: ComputeJob, event: dict) -> dict:
        """
        Create a bid for a P415 event
        """
        
        value = self.calculate_flexibility_value(job, 0, event)
        
        if value <= 0:
            return None
        
        bid = {
            'bid_id': f"bid_{datetime.now().timestamp()}",
            'job_id': job.id,
            'product': event['product'],
            'power_mw': job.power_mw,
            'duration_minutes': event.get('duration_minutes', 30),
            'bid_price': value / (job.power_mw * event.get('duration_minutes', 30) / 60),
            'expected_revenue': value,
            'timestamp': datetime.now().isoformat()
        }
        
        self.active_bids.append(bid)
        return bid
    
    def get_total_p415_potential(self, jobs: List[ComputeJob], 
                                events: List[dict]) -> float:
        """
        Calculate total potential P415 revenue for a set of jobs
        """
        
        total = 0.0
        for job in jobs:
            for event in events:
                value = self.calculate_flexibility_value(job, 0, event)
                total += value
        
        return total