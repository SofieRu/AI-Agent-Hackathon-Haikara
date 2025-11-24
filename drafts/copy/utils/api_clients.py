import aiohttp
import asyncio
from typing import List, Dict
import os

class NationalGridClient:
    """Client for National Grid Carbon Intensity API"""
    
    def __init__(self, api_key: str = None):
        self.base_url = "https://api.carbonintensity.org.uk"
        self.api_key = api_key
    
    async def get_carbon_intensity_forecast(self, hours: int = 24) -> Dict[str, List[float]]:
        """
        Fetch carbon intensity forecast
        Returns dict with region -> list of carbon values
        """
        
        # Map regions to National Grid region IDs
        region_mapping = {
            'north': 1,  # North England
            'south': 13,  # South England
            'scotland': 17  # Scotland
        }
        
        results = {}
        
        for region_name, region_id in region_mapping.items():
            try:
                url = f"{self.base_url}/regional/regionid/{region_id}"
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            # Parse and extend to full horizon
                            carbon_values = self._parse_carbon_data(data, hours)
                            results[region_name] = carbon_values
                        else:
                            # Fallback to mock data
                            results[region_name] = self._generate_mock_carbon(hours, region_name)
            except:
                # Fallback to mock data
                results[region_name] = self._generate_mock_carbon(hours, region_name)
        
        return results
    
    def _parse_carbon_data(self, data: dict, hours: int) -> List[float]:
        """Parse API response"""
        try:
            intensity_data = data.get('data', {}).get('data', [])
            values = [entry.get('intensity', {}).get('forecast', 200) for entry in intensity_data]
            # Extend to full horizon
            while len(values) < hours:
                values.append(values[-1] if values else 200)
            return values[:hours]
        except:
            return self._generate_mock_carbon(hours, 'default')
    
    def _generate_mock_carbon(self, hours: int, region: str) -> List[float]:
        """Generate mock carbon intensity data"""
        import random
        base = 150 if region == 'scotland' else 200
        return [base + random.uniform(-50, 100) for _ in range(hours)]
    
    async def get_current_carbon_intensity(self, region: str) -> float:
        """Get current carbon intensity"""
        forecast = await self.get_carbon_intensity_forecast(1)
        return forecast.get(region, [200])[0]


class OctopusEnergyClient:
    """Client for Octopus Energy pricing API"""
    
    def __init__(self, api_key: str = None):
        self.base_url = "https://api.octopus.energy/v1"
        self.api_key = api_key
    
    async def get_price_forecast(self, hours: int = 24) -> Dict[str, List[float]]:
        """
        Fetch electricity price forecast
        Returns dict with region -> list of prices in £/kWh
        """
        
        regions = ['north', 'south', 'scotland']
        results = {}
        
        for region in regions:
            try:
                # In production, fetch real Agile Octopus prices
                # For now, generate realistic mock data
                results[region] = self._generate_mock_prices(hours, region)
            except:
                results[region] = self._generate_mock_prices(hours, region)
        
        return results
    
    def _generate_mock_prices(self, hours: int, region: str) -> List[float]:
        """Generate realistic price patterns"""
        import random
        from datetime import datetime
        
        prices = []
        base_price = 0.15  # £/kWh
        
        for hour in range(hours):
            hour_of_day = (datetime.now().hour + hour) % 24
            
            # Peak pricing 17:00-20:00
            if 17 <= hour_of_day <= 20:
                multiplier = random.uniform(1.5, 2.2)
            # Off-peak 00:00-06:00
            elif 0 <= hour_of_day <= 6:
                multiplier = random.uniform(0.6, 0.8)
            else:
                multiplier = random.uniform(0.9, 1.3)
            
            # Scotland slightly cheaper
            if region == 'scotland':
                multiplier *= 0.95
            
            price = base_price * multiplier
            prices.append(round(price, 4))
        
        return prices
    
    async def get_current_price(self, region: str) -> float:
        """Get current electricity price"""
        forecast = await self.get_price_forecast(1)
        return forecast.get(region, [0.15])[0]