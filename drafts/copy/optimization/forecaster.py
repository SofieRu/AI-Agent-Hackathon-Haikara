import asyncio
from typing import List, Dict
from datetime import datetime, timedelta
from models.job import ComputeJob
import random
import json

class CarbonAwareForecaster:
    """
    Uses AI (GPT-4) to predict optimal execution windows
    Falls back to heuristics if API unavailable
    """
    
    def __init__(self, use_gpt4: bool = False):
        self.use_gpt4 = use_gpt4
        
        if use_gpt4:
            try:
                import openai
                self.openai_client = openai
                print("🤖 GPT-4 forecaster enabled")
            except:
                print("⚠️  OpenAI not available, using heuristic forecaster")
                self.use_gpt4 = False
        else:
            print("📊 Using heuristic forecaster")
    
    async def forecast_optimal_windows(self, job: ComputeJob, grid_forecast: dict, 
                                      horizon_hours: int = 24) -> List[dict]:
        """
        Predict optimal execution windows for a job
        Returns top 3 windows ranked by cost and carbon
        """
        
        if self.use_gpt4:
            return await self._gpt4_forecast(job, grid_forecast, horizon_hours)
        else:
            return await self._heuristic_forecast(job, grid_forecast, horizon_hours)
    
    async def _heuristic_forecast(self, job: ComputeJob, grid_forecast: dict, 
                                  horizon_hours: int) -> List[dict]:
        """
        Heuristic-based forecasting using grid data patterns
        """
        
        prices = grid_forecast['price']
        carbon = grid_forecast['carbon']
        
        # Find windows with lowest combined cost and carbon
        windows = []
        
        for region in ['north', 'south', 'scotland']:
            for hour in range(horizon_hours - int(job.duration_hours)):
                # Calculate average cost and carbon for this window
                window_price = sum(prices[region][hour:hour + int(job.duration_hours)]) / job.duration_hours
                window_carbon = sum(carbon[region][hour:hour + int(job.duration_hours)]) / job.duration_hours
                
                # Composite score (lower is better)
                score = (window_price / 0.15) * 0.5 + (window_carbon / 200) * 0.5
                
                windows.append({
                    'start_hour': hour,
                    'region': region,
                    'avg_price': window_price,
                    'avg_carbon': window_carbon,
                    'score': score,
                    'confidence': 0.7 + random.uniform(0, 0.2)  # Mock confidence
                })
        
        # Sort by score and return top 3
        windows.sort(key=lambda x: x['score'])
        return windows[:3]
    
    async def _gpt4_forecast(self, job: ComputeJob, grid_forecast: dict, 
                            horizon_hours: int) -> List[dict]:
        """
        GPT-4 based forecasting (requires OpenAI API key)
        """
        
        try:
            prompt = self._build_forecast_prompt(job, grid_forecast, horizon_hours)
            
            response = await self.openai_client.ChatCompletion.acreate(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert energy system optimizer."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            
            result = json.loads(response.choices[0].message.content)
            return result['optimal_windows']
            
        except Exception as e:
            print(f"⚠️  GPT-4 forecast failed: {e}, falling back to heuristics")
            return await self._heuristic_forecast(job, grid_forecast, horizon_hours)
    
    def _build_forecast_prompt(self, job: ComputeJob, grid_forecast: dict, 
                               horizon_hours: int) -> str:
        """Build prompt for GPT-4"""
        
        return f"""
Analyze this compute workload and grid forecast to find the 3 best execution windows.

Job Details:
- Type: {job.job_type}
- Energy Required: {job.energy_kwh} kWh
- Duration: {job.duration_hours} hours
- Deadline: {job.deadline.isoformat()}
- Can Defer: {job.can_defer}
- Flexibility: {job.flexibility_minutes} minutes

Grid Forecast (next {horizon_hours} hours):
{json.dumps(grid_forecast, indent=2)}

Return JSON with top 3 windows:
{{
  "optimal_windows": [
    {{
      "start_hour": 0,
      "region": "scotland",
      "avg_price": 0.12,
      "avg_carbon": 150,
      "score": 0.65,
      "confidence": 0.85,
      "reasoning": "Low carbon due to wind generation"
    }}
  ]
}}
"""