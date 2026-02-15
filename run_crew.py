#!/usr/bin/env python3
"""
CLI entry point for running the investment crew.

Usage:
    python run_crew.py --db-path market_data.duckdb
    python run_crew.py --focus ethereum solana
    python run_crew.py --no-save
"""

import argparse
import logging
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agents.crew import run_investment_crew


def setup_logging(verbose: bool = False):
    """Configure logging for the application."""
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'crew_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        ]
    )


def check_environment():
    """Check that required environment variables are set."""
    required_vars = {
        "OPENAI_API_KEY": "OpenAI API key for LLM access",
        "SERPER_API_KEY": "Serper API key for web search",
    }
    
    missing = []
    for var, description in required_vars.items():
        if not os.environ.get(var):
            missing.append(f"  - {var}: {description}")
    
    if missing:
        print("ERROR: Missing required environment variables:")
        print("\n".join(missing))
        print("\nSet these variables before running the crew.")
        print("\nExample:")
        print("  export OPENAI_API_KEY='your-key-here'")
        print("  export SERPER_API_KEY='your-key-here'")
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run the CrewAI investment analysis crew",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                              Run with default settings
  %(prog)s --db-path custom.duckdb     Use custom database
  %(prog)s --focus ethereum solana     Focus analysis on specific assets
  %(prog)s --no-save                   Run without saving report
  %(prog)s --verbose                   Enable debug logging
        """
    )
    
    parser.add_argument(
        "--db-path",
        default="market_data.duckdb",
        help="Path to DuckDB database (default: market_data.duckdb)"
    )
    
    parser.add_argument(
        "--focus",
        nargs="+",
        default=None,
        help="Specific assets to focus analysis on (e.g., ethereum solana)"
    )
    
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save the report to disk"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose debug logging"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=args.verbose)
    logger = logging.getLogger(__name__)
    
    # Check environment
    if not check_environment():
        sys.exit(1)
    
    # Check database exists
    if not os.path.exists(args.db_path):
        logger.warning(f"Database not found at {args.db_path}. A new database will be created.")
    
    # Run the crew
    print("\n" + "=" * 60)
    print("CrewAI Investment Analysis System")
    print("=" * 60)
    print(f"\nDatabase: {args.db_path}")
    if args.focus:
        print(f"Focus Assets: {', '.join(args.focus)}")
    print(f"Save Report: {not args.no_save}")
    print("\n" + "-" * 60)
    print("Starting crew execution...")
    print("-" * 60 + "\n")
    
    start_time = datetime.now()
    
    result = run_investment_crew(
        db_path=args.db_path,
        focus_assets=args.focus,
        save_report=not args.no_save,
    )
    
    # Print results
    print("\n" + "=" * 60)
    print("Execution Complete")
    print("=" * 60)
    
    if result.get("success"):
        print(f"\nStatus: SUCCESS")
        print(f"Execution Time: {result.get('execution_time_seconds', 0):.1f} seconds")
        
        if result.get("report_path"):
            print(f"Report Saved: {result['report_path']}")
        
        print("\n--- Crew Output ---\n")
        output = result.get("output", "")
        if hasattr(output, 'raw'):
            print(output.raw)
        else:
            print(str(output))
        
        sys.exit(0)
    else:
        print(f"\nStatus: FAILED")
        print(f"Error: {result.get('error', 'Unknown error')}")
        print(f"Execution Time: {result.get('execution_time_seconds', 0):.1f} seconds")
        sys.exit(1)


if __name__ == "__main__":
    main()
