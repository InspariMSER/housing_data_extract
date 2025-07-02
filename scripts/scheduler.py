"""
Housing Data Scheduler
Automatically runs the data pipeline at scheduled intervals
"""

import logging
import signal
import sys
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# Add src directory to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))
# Add pipeline directory to path  
sys.path.append(str(Path(__file__).parent.parent / 'pipeline'))

from run_pipeline import main as run_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log'),
        logging.StreamHandler()
    ]
)

class GracefulKiller:
    """Handle graceful shutdown on SIGTERM/SIGINT."""
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):
        logging.info(f"Received signal {signum}, shutting down gracefully...")
        self.kill_now = True

def scheduled_pipeline_run():
    """Run the pipeline and handle errors."""
    try:
        logging.info("Starting scheduled pipeline run...")
        success = run_pipeline()
        if success:
            logging.info("Scheduled pipeline run completed successfully")
        else:
            logging.error("Scheduled pipeline run failed")
    except Exception as e:
        logging.error(f"Error during scheduled pipeline run: {e}")

def main():
    """Main scheduler loop."""
    GracefulKiller()  # Set up signal handlers
    scheduler = BlockingScheduler()
    
    # Schedule daily runs at 6 AM
    scheduler.add_job(
        scheduled_pipeline_run,
        CronTrigger(hour=6, minute=0),
        id='daily_pipeline',
        name='Daily Housing Data Pipeline',
        replace_existing=True
    )
    
    # Also run every 4 hours during active hours (8 AM to 8 PM)
    scheduler.add_job(
        scheduled_pipeline_run,
        CronTrigger(hour='8,12,16,20', minute=0),
        id='active_hours_pipeline',
        name='Active Hours Housing Data Pipeline',
        replace_existing=True
    )
    
    logging.info("Scheduler started. Jobs scheduled:")
    scheduler.print_jobs()
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logging.info("Scheduler interrupted by user")
    finally:
        scheduler.shutdown()
        logging.info("Scheduler shut down")

if __name__ == "__main__":
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    # Run once immediately if requested
    if len(sys.argv) > 1 and sys.argv[1] == "--run-once":
        logging.info("Running pipeline once immediately...")
        success = run_pipeline()
        sys.exit(0 if success else 1)
    
    # Otherwise start scheduler
    main()
