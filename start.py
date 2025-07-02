#!/usr/bin/env python3
"""
Housing Data System Startup Script
Unified script to start different components of the housing data system
"""

import sys
import subprocess
import argparse
from pathlib import Path

def run_pipeline():
    """Run the data extraction and transformation pipeline."""
    print("🔄 Starting housing data pipeline...")
    result = subprocess.run([
        sys.executable, 
        str(Path(__file__).parent / "pipeline" / "run_pipeline.py")
    ])
    return result.returncode == 0

def start_app():
    """Start the Streamlit web application."""
    print("🚀 Starting Streamlit web application...")
    subprocess.run([
        sys.executable, "-m", "streamlit", "run", 
        str(Path(__file__).parent / "app" / "app_local.py"),
        "--server.headless=false"
    ])

def start_scheduler():
    """Start the automated scheduler."""
    print("⏰ Starting automated scheduler...")
    subprocess.run([
        sys.executable,
        str(Path(__file__).parent / "scripts" / "scheduler.py")
    ])

def run_full_setup():
    """Run pipeline once, then start the app."""
    print("🎯 Running full setup: Pipeline + App")
    success = run_pipeline()
    if success:
        print("✅ Pipeline completed successfully!")
        print("🚀 Starting web application...")
        start_app()
    else:
        print("❌ Pipeline failed!")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Housing Data System Control")
    parser.add_argument(
        "command", 
        choices=["pipeline", "app", "scheduler", "full"],
        help="Command to run: pipeline (run once), app (start webapp), scheduler (automated), full (pipeline + app)"
    )
    
    args = parser.parse_args()
    
    print("🏠 Housing Data System")
    print("=" * 50)
    
    if args.command == "pipeline":
        success = run_pipeline()
        sys.exit(0 if success else 1)
    elif args.command == "app":
        start_app()
    elif args.command == "scheduler":
        start_scheduler()
    elif args.command == "full":
        run_full_setup()

if __name__ == "__main__":
    main()
