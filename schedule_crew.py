#!/usr/bin/env python3
"""
Scheduled execution script for the investment crew.

This script is designed to be run by cron or a task scheduler.
It runs the investment crew with default settings and logs results.

Crontab example (run every Sunday at 9am):
    0 9 * * 0 /path/to/python /path/to/schedule_crew.py

Usage:
    python schedule_crew.py
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from agents.crew import run_investment_crew

# Configuration
DB_PATH = os.environ.get("DUCKDB_PATH", "market_data.duckdb")
LOG_DIR = PROJECT_ROOT / "logs"


def setup_logging():
    """Configure logging for scheduled execution."""
    LOG_DIR.mkdir(exist_ok=True)
    
    log_file = LOG_DIR / f"scheduled_crew_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ]
    )
    
    return logging.getLogger(__name__)


def check_environment():
    """Check that required environment variables are set."""
    required = ["OPENAI_API_KEY", "SERPER_API_KEY"]
    missing = [var for var in required if not os.environ.get(var)]
    
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")


def main():
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("Scheduled Investment Crew Execution")
    logger.info("=" * 60)
    logger.info(f"Start Time: {datetime.now().isoformat()}")
    logger.info(f"Database: {DB_PATH}")
    
    try:
        # Check environment
        check_environment()
        
        # Run the investment crew
        result = run_investment_crew(
            db_path=DB_PATH,
            focus_assets=None,  # Analyze all relevant assets
            save_report=True,
        )
        
        if result.get("success"):
            logger.info("Execution completed successfully")
            logger.info(f"Execution time: {result.get('execution_time_seconds', 0):.1f} seconds")
            logger.info(f"Report saved: {result.get('report_path', 'N/A')}")
            return 0
        else:
            logger.error(f"Execution failed: {result.get('error', 'Unknown error')}")
            return 1
            
    except EnvironmentError as e:
        logger.error(f"Environment error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1
    finally:
        logger.info(f"End Time: {datetime.now().isoformat()}")
        logger.info("=" * 60)


if __name__ == "__main__":
    sys.exit(main())
