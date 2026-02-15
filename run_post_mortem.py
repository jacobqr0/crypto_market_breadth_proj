#!/usr/bin/env python3
"""
CLI entry point for running the Post-Mortem Architect.

This runs SEPARATELY from the core investment crew. It analyzes
historical performance and generates meta-learning reports.

Usage:
    python run_post_mortem.py --db-path market_data.duckdb --period-months 1
    python run_post_mortem.py --period-months 3
    python run_post_mortem.py --no-save
"""

import argparse
import logging
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agents.post_mortem import run_meta_learning, get_performance_summary


def setup_logging(verbose: bool = False):
    """Configure logging for the application."""
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'post_mortem_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        ]
    )


def check_environment():
    """Check that required environment variables are set."""
    if not os.environ.get("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY environment variable not set")
        print("\nSet it before running:")
        print("  export OPENAI_API_KEY='your-key-here'")
        return False
    return True


def print_performance_summary(db_path: str):
    """Print a quick performance summary before analysis."""
    print("\n--- Current Portfolio Summary ---\n")
    
    try:
        summary = get_performance_summary(db_path)
        
        print(f"Total Positions: {summary.get('total_positions', 0)}")
        print(f"Total Cost Basis: ${summary.get('total_cost_basis_usd', 0):,.2f}")
        print(f"Total Realized P&L: ${summary.get('total_realized_pnl_usd', 0):,.2f}")
        print(f"Total Trades: {summary.get('total_trades', 0)} ({summary.get('total_buys', 0)} buys, {summary.get('total_sells', 0)} sells)")
        
        if summary.get('positions'):
            print("\nPositions:")
            for pos in summary['positions']:
                print(f"  - {pos['symbol'].upper()}: {pos['quantity']:.6f}")
        else:
            print("\nNo open positions.")
            
    except Exception as e:
        print(f"Could not retrieve portfolio summary: {e}")
    
    print("\n" + "-" * 40 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Run the Post-Mortem Architect meta-learning analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                              Run with default settings (1 month)
  %(prog)s --period-months 3            Analyze last 3 months
  %(prog)s --db-path custom.duckdb     Use custom database
  %(prog)s --no-save                   Run without saving report
  %(prog)s --summary-only              Just show performance summary
        """
    )
    
    parser.add_argument(
        "--db-path",
        default="market_data.duckdb",
        help="Path to DuckDB database (default: market_data.duckdb)"
    )
    
    parser.add_argument(
        "--period-months",
        type=int,
        default=1,
        help="Number of months to analyze (default: 1)"
    )
    
    parser.add_argument(
        "--reports-dir",
        default="reports",
        help="Directory containing investment reports (default: reports)"
    )
    
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save the meta-learning report to disk"
    )
    
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Only show performance summary, don't run full analysis"
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
    if not args.summary_only and not check_environment():
        sys.exit(1)
    
    # Print header
    print("\n" + "=" * 60)
    print("Post-Mortem Architect - Meta-Learning Analysis")
    print("=" * 60)
    print(f"\nDatabase: {args.db_path}")
    print(f"Analysis Period: {args.period_months} month(s)")
    print(f"Reports Directory: {args.reports_dir}")
    
    # Show performance summary
    print_performance_summary(args.db_path)
    
    if args.summary_only:
        print("Summary only mode - exiting without running analysis.")
        sys.exit(0)
    
    # Run the post-mortem analysis
    print("Starting Post-Mortem analysis...")
    print("-" * 60 + "\n")
    
    start_time = datetime.now()
    
    result = run_meta_learning(
        db_path=args.db_path,
        period_months=args.period_months,
        reports_dir=args.reports_dir,
        save_report=not args.no_save,
    )
    
    # Print results
    print("\n" + "=" * 60)
    print("Analysis Complete")
    print("=" * 60)
    
    if result.get("success"):
        print(f"\nStatus: SUCCESS")
        print(f"Execution Time: {result.get('execution_time_seconds', 0):.1f} seconds")
        
        if result.get("report_path"):
            print(f"Report Saved: {result['report_path']}")
        
        print("\n--- Post-Mortem Output ---\n")
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
