#!/usr/bin/env python3
"""
Scheduled execution script for the Post-Mortem Architect.

This script is designed to be run by cron or a task scheduler
on a monthly or quarterly basis for meta-learning analysis.

Crontab example (run on 1st of each month at 10am):
    0 10 1 * * /path/to/python /path/to/schedule_post_mortem.py

Usage:
    python schedule_post_mortem.py
    python schedule_post_mortem.py --period-months 3
"""

import argparse
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from agents.post_mortem import run_meta_learning

# Configuration
DB_PATH = os.environ.get("DUCKDB_PATH", "market_data.duckdb")
LOG_DIR = PROJECT_ROOT / "logs"


def setup_logging():
    """Configure logging for scheduled execution."""
    LOG_DIR.mkdir(exist_ok=True)
    
    log_file = LOG_DIR / f"scheduled_post_mortem_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
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
    if not os.environ.get("OPENAI_API_KEY"):
        raise EnvironmentError("OPENAI_API_KEY environment variable not set")


def main():
    parser = argparse.ArgumentParser(
        description="Scheduled Post-Mortem Architect execution"
    )
    parser.add_argument(
        "--period-months",
        type=int,
        default=1,
        help="Number of months to analyze (default: 1)"
    )
    args = parser.parse_args()
    
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("Scheduled Post-Mortem Architect Execution")
    logger.info("=" * 60)
    logger.info(f"Start Time: {datetime.now().isoformat()}")
    logger.info(f"Database: {DB_PATH}")
    logger.info(f"Analysis Period: {args.period_months} month(s)")
    
    try:
        # Check environment
        check_environment()
        
        # Run the post-mortem analysis
        result = run_meta_learning(
            db_path=DB_PATH,
            period_months=args.period_months,
            reports_dir="reports",
            save_report=True,
        )
        
        if result.get("success"):
            logger.info("Analysis completed successfully")
            logger.info(f"Execution time: {result.get('execution_time_seconds', 0):.1f} seconds")
            logger.info(f"Report saved: {result.get('report_path', 'N/A')}")
            return 0
        else:
            logger.error(f"Analysis failed: {result.get('error', 'Unknown error')}")
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
