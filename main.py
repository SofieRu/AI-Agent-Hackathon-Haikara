import asyncio
import os
from dotenv import load_dotenv
from agents.orchestrator import OrchestratorAgent
import signal
import sys

# Load environment variables
load_dotenv()

def load_config():
    """Load configuration from environment"""
    return {
        'bap_url': os.getenv('BAP_SANDBOX_URL', 'https://bap-sandbox.beckn.org'),
        'subscriber_id': os.getenv('SUBSCRIBER_ID', 'flex-compute-scheduler'),
        'private_key': os.getenv('PRIVATE_KEY_PATH', ''),
        'carbon_cap': float(os.getenv('CARBON_CAP', 100)),
        'optimization_horizon': int(os.getenv('OPTIMIZATION_HORIZON_HOURS', 24)),
        'cycle_interval': int(os.getenv('CYCLE_INTERVAL_SECONDS', 300))
    }

async def main():
    """Main application entry point"""
    
    print("="*80)
    print("🌟 FLEXIBLE COMPUTE SCHEDULER FOR DIGITAL ENERGY GRID")
    print("="*80)
    print()
    
    # Load configuration
    config = load_config()
    
    print("📋 Configuration:")
    print(f"   BAP URL: {config['bap_url']}")
    print(f"   Subscriber ID: {config['subscriber_id']}")
    print(f"   Carbon Cap: {config['carbon_cap']} gCO2/kWh")
    print(f"   Optimization Horizon: {config['optimization_horizon']} hours")
    print(f"   Cycle Interval: {config['cycle_interval']} seconds")
    print()
    
    # Initialize orchestrator
    orchestrator = OrchestratorAgent(
        bap_url=config['bap_url'],
        subscriber_id=config['subscriber_id'],
        private_key=config['private_key'],
        carbon_cap=config['carbon_cap'],
        optimization_horizon=config['optimization_horizon']
    )
    
    # Setup graceful shutdown
    def signal_handler(sig, frame):
        print("\n\n🛑 Shutting down gracefully...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    print("\n✅ System ready! Starting orchestration loop...")
    print("   Press Ctrl+C to stop\n")
    
    # Main orchestration loop
    cycle_count = 0
    while True:
        cycle_count += 1
        print(f"\n{'='*80}")
        print(f"🔄 CYCLE #{cycle_count} - {asyncio.get_event_loop().time():.0f}s")
        print(f"{'='*80}")
        
        try:
            await orchestrator.run_cycle()
        except Exception as e:
            print(f"❌ Cycle error: {e}")
            import traceback
            traceback.print_exc()
        
        # Wait before next cycle
        print(f"\n⏳ Waiting {config['cycle_interval']} seconds until next cycle...")
        await asyncio.sleep(config['cycle_interval'])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")