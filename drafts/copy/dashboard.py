from flask import Flask, render_template, jsonify
from flask_cors import CORS
import plotly.graph_objs as go
import plotly
import json
from datetime import datetime
import os

app = Flask(__name__)
CORS(app)

# Mock data store (in production, connect to orchestrator)
class DashboardData:
    def __init__(self):
        self.metrics = {
            'total_cost_saved': 12540.50,
            'carbon_reduced_tons': 2.3,
            'p415_revenue': 3200.00,
            'jobs_scheduled': 147,
            'avg_defer_time_mins': 180
        }
        
        self.recent_jobs = []
        self.schedule = []

dashboard_data = DashboardData()

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    """Get current system metrics"""
    return jsonify(dashboard_data.metrics)

@app.route('/api/schedule')
def get_schedule():
    """Get current schedule visualization data"""
    
    # Create sample schedule visualization
    fig = go.Figure()
    
    # Sample data
    jobs = [
        {'name': 'ML Training', 'start': 2, 'duration': 4, 'region': 'scotland', 'carbon': 80},
        {'name': 'Batch Inference', 'start': 6, 'duration': 2, 'region': 'south', 'carbon': 120},
        {'name': 'Analytics', 'start': 10, 'duration': 3, 'region': 'north', 'carbon': 95},
        {'name': 'Data Processing', 'start': 14, 'duration': 2, 'region': 'scotland', 'carbon': 70},
    ]
    
    for job in jobs:
        color = 'green' if job['carbon'] < 100 else 'orange'
        fig.add_trace(go.Bar(
            x=[job['duration']],
            y=[job['name']],
            orientation='h',
            name=job['name'],
            marker=dict(color=color),
            text=f"{job['region']}<br>{job['carbon']} gCO2/kWh",
            textposition='inside',
            base=job['start']
        ))
    
    fig.update_layout(
        title='Optimized Compute Schedule (Next 24 Hours)',
        xaxis_title='Hour of Day',
        yaxis_title='Job',
        barmode='overlay',
        height=400,
        showlegend=False
    )
    
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

@app.route('/api/carbon_forecast')
def get_carbon_forecast():
    """Get carbon intensity forecast"""
    
    import random
    hours = list(range(24))
    
    fig = go.Figure()
    
    regions = ['north', 'south', 'scotland']
    for region in regions:
        carbon_values = [150 + random.uniform(-50, 100) for _ in hours]
        fig.add_trace(go.Scatter(
            x=hours,
            y=carbon_values,
            mode='lines+markers',
            name=region.capitalize()
        ))
    
    # Add carbon cap line
    fig.add_hline(y=100, line_dash="dash", line_color="red", 
                  annotation_text="Carbon Cap")
    
    fig.update_layout(
        title='Carbon Intensity Forecast by Region',
        xaxis_title='Hour',
        yaxis_title='Carbon Intensity (gCO2/kWh)',
        height=400
    )
    
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

@app.route('/api/price_forecast')
def get_price_forecast():
    """Get price forecast"""
    
    import random
    hours = list(range(24))
    
    fig = go.Figure()
    
    regions = ['north', 'south', 'scotland']
    for region in regions:
        prices = [0.15 + random.uniform(-0.05, 0.10) for _ in hours]
        fig.add_trace(go.Scatter(
            x=hours,
            y=prices,
            mode='lines+markers',
            name=region.capitalize()
        ))
    
    fig.update_layout(
        title='Electricity Price Forecast by Region',
        xaxis_title='Hour',
        yaxis_title='Price (£/kWh)',
        height=400
    )
    
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

@app.route('/api/savings')
def get_savings():
    """Get savings comparison"""
    
    categories = ['Without Optimization', 'With Optimization']
    cost = [18500, 12540]
    carbon = [4.2, 2.3]
    
    fig = go.Figure()
    fig.add_trace(go.Bar(name='Cost (£)', x=categories, y=cost))
    fig.add_trace(go.Bar(name='Carbon (tons CO2)', x=categories, y=carbon))
    
    fig.update_layout(
        title='Cost and Carbon Savings',
        barmode='group',
        height=400
    )
    
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', 5000))
    print(f"🌐 Dashboard starting on http://localhost:{port}")
    app.run(debug=True, host='0.0.0.0', port=port)