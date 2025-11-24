"""
Grid Data Ingestor Agent - Fetches real-time grid data
Integrates with National Grid Carbon API and Octopus Energy pricing
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import random
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.api_clients import NationalGridClient, OctopusEnergyClient


class GridDataIngestor:
    """
    Ingests real-time grid data: prices, carbon intensity, P415 events
    
    Data sources:
    - National Grid Carbon Intensity API
    - Octopus Energy Agile Pricing API
    - P415 flexibility market data (simulated)
    
    Implements caching to reduce API calls
    """
    
    def __init__(self, cache_duration_minutes: int = 5):
        """
        Initialize grid data ingestor
        
        Args:
            cache_duration_minutes: How long to cache data before refreshing
        """
        
        # Initialize API clients
        self.carbon_client = NationalGridClient()
        self.price_client = OctopusEnergyClient()
        
        # Cache management
        self.last_forecast = None
        self.last_update = None
        self.cache_duration = timedelta(minutes=cache_duration_minutes)
        
        print(f"✅ Grid Data Ingestor initialized (cache: {cache_duration_minutes} min)")
    
    async def fetch_forecast(self, horizon_hours: int = 24) -> dict:
        """
        Fetch comprehensive grid forecast
        
        Returns unified forecast containing:
        - Price data by region and hour
        - Carbon intensity by region and hour
        - P415 flexibility events
        - Statistical summaries
        
        Args:
            horizon_hours: Forecast horizon in hours
        
        Returns:
            Dictionary with complete grid forecast
        """
        
        # Check cache first
        if self._is_cache_valid():
            print("   ℹ️  Using cached grid forecast")
            return self.last_forecast
        
        print(f"   🌍 Fetching fresh grid forecast ({horizon_hours}h horizon)...")
        
        try:
            # Fetch all data in parallel
            carbon_task = self.carbon_client.get_carbon_intensity_forecast(horizon_hours)
            price_task = self.price_client.get_price_forecast(horizon_hours)
            p415_task = self._fetch_p415_events(horizon_hours)
            
            carbon_data, price_data, p415_events = await asyncio.gather(
                carbon_task, price_task, p415_task,
                return_exceptions=True
            )
            
            # Handle exceptions
            if isinstance(carbon_data, Exception):
                print(f"   ⚠️  Carbon API error: {carbon_data}, using fallback")
                carbon_data = self._generate_mock_carbon(horizon_hours)
            
            if isinstance(price_data, Exception):
                print(f"   ⚠️  Price API error: {price_data}, using fallback")
                price_data = self._generate_mock_prices(horizon_hours)
            
            if isinstance(p415_events, Exception):
                print(f"   ⚠️  P415 fetch error: {p415_events}, using fallback")
                p415_events = []
            
            # Build comprehensive forecast
            forecast = {
                'price': price_data,
                'carbon': carbon_data,
                'p415_events': p415_events,
                'avg_price': self._calculate_average(price_data),
                'avg_carbon': self._calculate_average(carbon_data),
                'min_carbon_hour': self._find_min_hour(carbon_data),
                'min_price_hour': self._find_min_hour(price_data),
                'timestamp': datetime.now().isoformat(),
                'horizon_hours': horizon_hours
            }
            
            # Update cache
            self.last_forecast = forecast
            self.last_update = datetime.now()
            
            print(f"   ✓ Grid forecast updated successfully")
            
            return forecast
            
        except Exception as e:
            print(f"   ❌ Grid forecast fetch failed: {e}")
            # Return mock data as ultimate fallback
            return self._generate_complete_mock_forecast(horizon_hours)
    
    def _is_cache_valid(self) -> bool:
        """Check if cached data is still valid"""
        if not self.last_forecast or not self.last_update:
            return False
        
        age = datetime.now() - self.last_update
        return age < self.cache_duration
    
    async def _fetch_p415_events(self, horizon_hours: int) -> List[dict]:
        """
        Fetch P415 flexibility market events
        
        P415 Products:
        - Dynamic Containment (DC): Fast frequency response (1s)
        - Dynamic Moderation (DM): Slower response (5 min)
        - Dynamic Regulation (DR): Continuous balancing (10 min)
        
        In production: Connect to National Grid ESO API
        For demo: Generate realistic mock events
        
        Args:
            horizon_hours: Forecast horizon
        
        Returns:
            List of P415 flexibility events
        """
        
        events = []
        products = ['DC', 'DM', 'DR']
        regions = ['north', 'south', 'scotland']
        
        # Generate 3-8 random events across the horizon
        num_events = random.randint(3, 8)
        
        for _ in range(num_events):
            event_hour = random.randint(0, horizon_hours - 1)
            product = random.choice(products)
            region = random.choice(regions)
            
            # Product-specific parameters
            if product == 'DC':
                clearing_price = random.uniform(100, 250)  # £/MW/h - high value
                duration_minutes = 30
                volume_mw = random.uniform(10, 50)
            elif product == 'DM':
                clearing_price = random.uniform(50, 150)  # £/MW/h - medium value
                duration_minutes = 60
                volume_mw = random.uniform(20, 80)
            else:  # DR
                clearing_price = random.uniform(20, 100)  # £/MW/h - lower value
                duration_minutes = 120
                volume_mw = random.uniform(30, 100)
            
            events.append({
                'time': event_hour,
                'region': region,
                'product': product,
                'product_name': {
                    'DC': 'Dynamic Containment',
                    'DM': 'Dynamic Moderation',
                    'DR': 'Dynamic Regulation'
                }[product],
                'clearing_price': clearing_price,
                'duration_minutes': duration_minutes,
                'volume_mw': volume_mw,
                'start_time': (datetime.now() + timedelta(hours=event_hour)).isoformat()
            })
        
        # Sort by time
        events.sort(key=lambda x: x['time'])
        
        return events
    
    def _generate_mock_carbon(self, hours: int) -> Dict[str, List[float]]:
        """
        Generate realistic mock carbon intensity data
        
        Patterns:
        - Daily cycle: Lower at night (more wind), higher during day
        - Regional variation: Scotland has more renewables (lower carbon)
        - Stochastic variation: Random fluctuations
        
        Args:
            hours: Number of hours to generate
        
        Returns:
            Dictionary mapping region -> list of carbon intensities
        """
        
        regions = {
            'north': 200,     # Base carbon intensity (gCO2/kWh)
            'south': 220,
            'scotland': 150   # More renewables
        }
        
        result = {}
        
        for region, base_carbon in regions.items():
            carbon_values = []
            
            for hour in range(hours):
                hour_of_day = (datetime.now().hour + hour) % 24
                
                # Night time: more wind energy (lower carbon)
                if 0 <= hour_of_day <= 6:
                    time_multiplier = random.uniform(0.5, 0.8)
                # Peak demand (17-20): higher carbon
                elif 17 <= hour_of_day <= 20:
                    time_multiplier = random.uniform(1.2, 1.6)
                else:
                    time_multiplier = random.uniform(0.8, 1.2)
                
                # Add random variation (renewable intermittency)
                random_factor = random.uniform(0.9, 1.1)
                
                carbon = base_carbon * time_multiplier * random_factor
                carbon_values.append(round(carbon, 2))
            
            result[region] = carbon_values
        
        return result
    
    def _generate_mock_prices(self, hours: int) -> Dict[str, List[float]]:
        """
        Generate realistic mock electricity price data
        
        Patterns:
        - Peak pricing: 17:00-20:00 (high demand)
        - Off-peak: 00:00-06:00 (low demand)
        - Regional variation: Scotland slightly cheaper
        - Market volatility: Random fluctuations
        
        Args:
            hours: Number of hours to generate
        
        Returns:
            Dictionary mapping region -> list of prices (£/kWh)
        """
        
        base_price = 0.15  # £/kWh baseline
        
        regions = {
            'north': 1.0,
            'south': 1.05,
            'scotland': 0.95
        }
        
        result = {}
        
        for region, regional_multiplier in regions.items():
            prices = []
            
            for hour in range(hours):
                hour_of_day = (datetime.now().hour + hour) % 24
                
                # Time-of-use pricing
                if 17 <= hour_of_day <= 20:
                    # Peak hours: expensive
                    time_multiplier = random.uniform(1.8, 2.5)
                elif 0 <= hour_of_day <= 6:
                    # Off-peak: cheap
                    time_multiplier = random.uniform(0.5, 0.7)
                else:
                    # Standard
                    time_multiplier = random.uniform(0.9, 1.3)
                
                # Market volatility
                volatility = random.uniform(0.95, 1.05)
                
                price = base_price * regional_multiplier * time_multiplier * volatility
                prices.append(round(price, 4))
            
            result[region] = prices
        
        return result
    
    def _generate_complete_mock_forecast(self, hours: int) -> dict:
        """Generate complete mock forecast as fallback"""
        
        carbon_data = self._generate_mock_carbon(hours)
        price_data = self._generate_mock_prices(hours)
        p415_events = asyncio.run(self._fetch_p415_events(hours))
        
        return {
            'price': price_data,
            'carbon': carbon_data,
            'p415_events': p415_events,
            'avg_price': self._calculate_average(price_data),
            'avg_carbon': self._calculate_average(carbon_data),
            'min_carbon_hour': self._find_min_hour(carbon_data),
            'min_price_hour': self._find_min_hour(price_data),
            'timestamp': datetime.now().isoformat(),
            'horizon_hours': hours,
            'data_source': 'mock'
        }
    
    def _calculate_average(self, data: Dict[str, List[float]]) -> float:
        """Calculate average across all regions and hours"""
        all_values = []
        for region_data in data.values():
            all_values.extend(region_data)
        return sum(all_values) / len(all_values) if all_values else 0.0
    
    def _find_min_hour(self, data: Dict[str, List[float]]) -> Dict[str, dict]:
        """Find hour with minimum value for each region"""
        result = {}
        for region, values in data.items():
            if values:
                min_value = min(values)
                min_hour = values.index(min_value)
                result[region] = {
                    'hour': min_hour,
                    'value': min_value
                }
        return result
    
    async def get_real_time_carbon(self, region: str) -> float:
        """
        Get current carbon intensity for specific region
        
        Args:
            region: Region name ('north', 'south', 'scotland')
        
        Returns:
            Current carbon intensity in gCO2/kWh
        """
        try:
            return await self.carbon_client.get_current_carbon_intensity(region)
        except:
            # Fallback to mock
            return random.uniform(100, 400)
    
    async def get_real_time_price(self, region: str) -> float:
        """
        Get current electricity price for specific region
        
        Args:
            region: Region name ('north', 'south', 'scotland')
        
        Returns:
            Current price in £/kWh
        """
        try:
            return await self.price_client.get_current_price(region)
        except:
            # Fallback to mock
            return random.uniform(0.10, 0.30)
    
    def clear_cache(self):
        """Manually clear cache to force fresh data fetch"""
        self.last_forecast = None
        self.last_update = None
        print("   🗑️  Grid forecast cache cleared")


if __name__ == "__main__":
    """Test the grid data ingestor independently"""
    
    async def test():
        ingestor = GridDataIngestor()
        
        print("\n=== Testing Grid Data Ingestor ===\n")
        
        # Test forecast fetch
        print("1. Fetching grid forecast...")
        forecast = await ingestor.fetch_forecast(24)
        
        print(f"\n   Forecast Summary:")
        print(f"   - Average Carbon: {forecast['avg_carbon']:.2f} gCO2/kWh")
        print(f"   - Average Price: £{forecast['avg_price']:.4f}/kWh")
        print(f"   - P415 Events: {len(forecast['p415_events'])}")
        
        print(f"\n   Best Hours (Lowest Carbon):")
        for region, info in forecast['min_carbon_hour'].items():
            print(f"   - {region}: Hour {info['hour']} ({info['value']:.1f} gCO2/kWh)")
        
        print(f"\n   P415 Opportunities:")
        for event in forecast['p415_events'][:3]:
            print(f"   - Hour {event['time']}: {event['product_name']} in {event['region']}, "
                  f"£{event['clearing_price']:.2f}/MW")
        
        # Test caching
        print("\n2. Testing cache...")
        forecast2 = await ingestor.fetch_forecast(24)
        print(f"   Cache hit: {forecast2 == forecast}")
        
        # Test real-time data
        print("\n3. Testing real-time data...")
        carbon = await ingestor.get_real_time_carbon('scotland')
        price = await ingestor.get_real_time_price('scotland')
        print(f"   Scotland now: {carbon:.1f} gCO2/kWh, £{price:.4f}/kWh")
    
    asyncio.run(test())