#!/usr/bin/env python3
"""
Script to add a SOL buy trade to the portfolio.

This script records a buy trade for Solana (SOL) and automatically
updates the position using the PortfolioStore methods.
"""

import sys
from pathlib import Path
from datetime import datetime

# Handle imports - works whether run from source/ or project root
try:
    from portfolio_store import PortfolioStore
except ImportError:
    from source.portfolio_store import PortfolioStore

def main():
    # Determine database path - check project root first, then current directory
    project_root = Path(__file__).parent.parent
    db_paths = [
        project_root / "market_data.duckdb",
        Path("market_data.duckdb")
    ]
    
    db_path = None
    for path in db_paths:
        if path.exists():
            db_path = str(path)
            break
    
    if db_path is None:
        # Use default path (will create if doesn't exist)
        db_path = str(project_root / "market_data.duckdb")
    
    # Initialize the portfolio store
    store = PortfolioStore(db_path)
    
    try:
        # Parse the trade execution time
        # Format: 1/19/26 22:32:47 (M/D/YY HH:MM:SS)
        executed_at = datetime.strptime("2/11/26 21:37:49", "%m/%d/%y %H:%M:%S")
        
        # Trade details
        asset_id = "bitcoin"  # CoinGecko asset ID 
        symbol = "btc"       # Trading symbol
        quantity = 0.00147628 # Amount in token
        price_usd = 67380   # Limit price per token
        fees_usd = 1.19      # Transaction fees
        
        # Record the buy trade
        # This will automatically:
        # 1. Insert the trade into the trades table
        # 2. Create or update the position in the positions table
        # 3. Calculate the new average cost basis
        trade_id = store.record_buy_trade(
            asset_id=asset_id,
            symbol=symbol,
            quantity=quantity,
            price_usd=price_usd,
            executed_at=executed_at,
            fees_usd=fees_usd
        )
        
        print(f"✓ Successfully recorded buy trade")
        print(f"  Trade ID: {trade_id}")
        print(f"  Asset: {symbol} ({asset_id})")
        print(f"  Quantity: {quantity} SOL")
        print(f"  Price: ${price_usd:.2f} per SOL")
        print(f"  Fees: ${fees_usd:.2f}")
        print(f"  Executed at: {executed_at}")
        
        # Verify the position was created/updated
        position = store.get_position(asset_id)
        if position:
            print(f"\n✓ Position updated:")
            print(f"  Current quantity: {position['quantity']} SOL")
            print(f"  Average cost basis: ${position['avg_cost_basis_usd']:.2f} per SOL")
            print(f"  Total cost basis: ${position['quantity'] * position['avg_cost_basis_usd']:.2f}")
        else:
            print("\n⚠ Warning: Position not found after trade")
            
    except Exception as e:
        print(f"✗ Error recording trade: {e}")
        raise
    finally:
        store.close()

if __name__ == "__main__":
    main()
