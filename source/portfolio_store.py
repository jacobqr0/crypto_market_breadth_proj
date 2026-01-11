"""
Portfolio persistence layer for crypto position and trade management.

This module provides a PortfolioStore class that handles all database operations
for tracking crypto positions, recording trades (buys/sells), and calculating
realized gains and losses.

=============================================================================
AGENT USAGE GUIDE
=============================================================================

This module is designed to be safe for autonomous AI agent usage.

SAFE OPERATIONS (read-only, no side effects):
    - get_open_positions()      → List of all positions with quantity > 0
    - get_position(asset_id)    → Single position details or None
    - get_trade_history()       → Full trade ledger (optionally filtered)
    - get_realized_pnl_summary() → Aggregated P&L statistics

MUTATION OPERATIONS (transactional, validated):
    - record_buy_trade(...)     → Records a buy and updates position
    - record_sell_trade(...)    → Records a sell, validates quantity, calculates P&L

SAFETY GUARANTEES:
    1. All writes are transactional (atomic commit or full rollback)
    2. Inputs are validated before any database changes
    3. Trades are append-only (immutable after creation)
    4. Sell operations validate sufficient quantity exists
    5. All functions have explicit, documented failure modes

EXAMPLE USAGE:
    from source.portfolio_store import PortfolioStore
    from datetime import datetime

    store = PortfolioStore("market_data.duckdb")

    # Record a buy
    store.record_buy_trade(
        asset_id="bitcoin",
        symbol="btc",
        quantity=0.5,
        price_usd=45000.0,
        executed_at=datetime.now()
    )

    # Check positions
    positions = store.get_open_positions()

    # Record a sell
    store.record_sell_trade(
        asset_id="bitcoin",
        symbol="btc",
        quantity=0.25,
        price_usd=50000.0,
        executed_at=datetime.now()
    )

    # Check realized P&L
    pnl = store.get_realized_pnl_summary()

    store.close()

=============================================================================
"""

import duckdb
import logging
import uuid
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)


# =============================================================================
# Dataclasses (typed return values)
# =============================================================================

@dataclass
class Position:
    """
    Represents a current crypto position (net holdings).
    
    Attributes:
        asset_id: CoinGecko asset identifier (e.g., "bitcoin")
        symbol: Trading symbol (e.g., "btc")
        quantity: Net quantity currently held
        avg_cost_basis_usd: Weighted average cost per unit
        market_value_usd: Current market value (may be None if not updated)
        unrealized_pnl_usd: Unrealized profit/loss (may be None if not calculated)
        opened_at: When position was first opened
        last_updated_at: Last modification timestamp
    """
    asset_id: str
    symbol: str
    quantity: float
    avg_cost_basis_usd: float
    market_value_usd: Optional[float]
    unrealized_pnl_usd: Optional[float]
    opened_at: datetime
    last_updated_at: datetime


@dataclass
class Trade:
    """
    Represents a single trade execution (buy or sell).
    
    Attributes:
        trade_id: Unique identifier (UUID)
        asset_id: CoinGecko asset identifier
        symbol: Trading symbol
        side: "BUY" or "SELL"
        quantity: Amount traded
        price_usd: Execution price per unit
        trade_value_usd: Total trade value (quantity * price)
        executed_at: When trade was executed
        fees_usd: Transaction fees
        realized_pnl_usd: Realized P&L (only populated for SELL trades)
        created_at: When record was created
    """
    trade_id: str
    asset_id: str
    symbol: str
    side: str
    quantity: float
    price_usd: float
    trade_value_usd: float
    executed_at: datetime
    fees_usd: float
    realized_pnl_usd: Optional[float]
    created_at: datetime


# =============================================================================
# Constants
# =============================================================================

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"


# =============================================================================
# PortfolioStore Class
# =============================================================================

class PortfolioStore:
    """
    DuckDB-backed storage for crypto portfolio management.
    
    Handles position tracking, trade recording, and P&L calculations
    with transactional guarantees for safe, restartable operations.
    
    Thread Safety:
        Not thread-safe. Each thread should use its own PortfolioStore instance.
    
    Usage:
        store = PortfolioStore("market_data.duckdb")
        try:
            store.record_buy_trade(...)
            positions = store.get_open_positions()
        finally:
            store.close()
    """
    
    def __init__(self, db_path: str = "market_data.duckdb"):
        """
        Initialize DuckDB connection and create schema.
        
        :param db_path: Path to DuckDB database file. Use ':memory:' for in-memory DB.
        
        Side Effects:
            - Creates database file if it doesn't exist
            - Creates positions and trades tables if they don't exist
            - Creates indexes for query optimization
        """
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_schema()
    
    def _initialize_schema(self):
        """
        Create all required tables and indexes if they don't exist.
        
        Tables created:
            - positions: Current holdings (one row per asset)
            - trades: Immutable trade ledger (append-only)
        
        This method is idempotent and safe to call multiple times.
        """
        
        # Positions table - current holdings
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                asset_id VARCHAR PRIMARY KEY,
                symbol VARCHAR NOT NULL,
                quantity DOUBLE NOT NULL DEFAULT 0,
                avg_cost_basis_usd DOUBLE NOT NULL DEFAULT 0,
                market_value_usd DOUBLE,
                unrealized_pnl_usd DOUBLE,
                opened_at TIMESTAMP NOT NULL,
                last_updated_at TIMESTAMP NOT NULL
            )
        """)
        
        # Trades table - immutable trade ledger
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id VARCHAR PRIMARY KEY,
                asset_id VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                side VARCHAR NOT NULL,
                quantity DOUBLE NOT NULL,
                price_usd DOUBLE NOT NULL,
                trade_value_usd DOUBLE NOT NULL,
                executed_at TIMESTAMP NOT NULL,
                fees_usd DOUBLE DEFAULT 0,
                realized_pnl_usd DOUBLE,
                created_at TIMESTAMP NOT NULL
            )
        """)
        
        # Create indexes for common query patterns
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_asset 
            ON trades(asset_id)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_executed 
            ON trades(executed_at)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_side 
            ON trades(side)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_positions_quantity 
            ON positions(quantity)
        """)
        
        logger.info("Portfolio schema initialized successfully")
    
    def close(self):
        """
        Close database connection.
        
        Always call this when done with the store to release resources.
        After closing, the store instance should not be reused.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
    
    @contextmanager
    def transaction(self):
        """
        Context manager for transactional operations.
        
        Ensures atomic execution: either all operations succeed and commit,
        or all operations are rolled back on any error.
        
        Usage:
            with store.transaction():
                store._insert_trade(...)
                store._update_position(...)
        
        Raises:
            Exception: Re-raises any exception after rollback
        """
        try:
            self.conn.execute("BEGIN TRANSACTION")
            yield
            self.conn.execute("COMMIT")
        except Exception as e:
            self.conn.execute("ROLLBACK")
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
    
    # =========================================================================
    # Write Operations (Agent-Facing API)
    # =========================================================================
    
    def record_buy_trade(
        self,
        asset_id: str,
        symbol: str,
        quantity: float,
        price_usd: float,
        executed_at: datetime,
        fees_usd: float = 0.0
    ) -> str:
        """
        Record a BUY trade and update the position.
        
        This operation is transactional: the trade is inserted and the position
        is updated atomically. If any step fails, all changes are rolled back.
        
        :param asset_id: CoinGecko asset identifier (e.g., "bitcoin")
        :param symbol: Trading symbol (e.g., "btc")
        :param quantity: Amount purchased (must be > 0)
        :param price_usd: Price per unit in USD (must be > 0)
        :param executed_at: When the trade was executed
        :param fees_usd: Transaction fees in USD (default 0.0)
        
        :return: The generated trade_id (UUID string)
        
        :raises ValueError: If quantity <= 0 or price_usd <= 0
        
        Side Effects:
            - Inserts a new trade record
            - Creates or updates position for this asset
            - Recalculates average cost basis
        
        Example:
            trade_id = store.record_buy_trade(
                asset_id="bitcoin",
                symbol="btc",
                quantity=0.5,
                price_usd=45000.0,
                executed_at=datetime.now()
            )
        """
        # Validate inputs
        if quantity <= 0:
            raise ValueError(f"quantity must be positive, got {quantity}")
        if price_usd <= 0:
            raise ValueError(f"price_usd must be positive, got {price_usd}")
        
        trade_id = str(uuid.uuid4())
        trade_value = quantity * price_usd
        now = datetime.now()
        
        with self.transaction():
            # Insert trade record
            self.conn.execute("""
                INSERT INTO trades (
                    trade_id, asset_id, symbol, side, quantity,
                    price_usd, trade_value_usd, executed_at,
                    fees_usd, realized_pnl_usd, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
            """, [
                trade_id, asset_id, symbol, SIDE_BUY, quantity,
                price_usd, trade_value, executed_at,
                fees_usd, now
            ])
            
            # Update or create position
            self._update_position_on_buy(asset_id, symbol, quantity, price_usd, now)
        
        logger.info(f"Recorded BUY trade {trade_id}: {quantity} {symbol} @ ${price_usd}")
        return trade_id
    
    def record_sell_trade(
        self,
        asset_id: str,
        symbol: str,
        quantity: float,
        price_usd: float,
        executed_at: datetime,
        fees_usd: float = 0.0
    ) -> str:
        """
        Record a SELL trade, calculate realized P&L, and update the position.
        
        This operation is transactional: the trade is inserted and the position
        is updated atomically. If any step fails, all changes are rolled back.
        
        P&L Calculation (Average Cost Method):
            cost_basis = quantity * avg_cost_basis_usd
            proceeds = quantity * price_usd
            realized_pnl = proceeds - cost_basis - fees_usd
        
        :param asset_id: CoinGecko asset identifier (e.g., "bitcoin")
        :param symbol: Trading symbol (e.g., "btc")
        :param quantity: Amount sold (must be > 0)
        :param price_usd: Price per unit in USD (must be > 0)
        :param executed_at: When the trade was executed
        :param fees_usd: Transaction fees in USD (default 0.0)
        
        :return: The generated trade_id (UUID string)
        
        :raises ValueError: If quantity <= 0 or price_usd <= 0
        :raises ValueError: If position doesn't exist or has insufficient quantity
        
        Side Effects:
            - Inserts a new trade record with realized P&L
            - Reduces position quantity (or closes if fully sold)
            - Position is deleted if quantity reaches 0
        
        Example:
            trade_id = store.record_sell_trade(
                asset_id="bitcoin",
                symbol="btc",
                quantity=0.25,
                price_usd=50000.0,
                executed_at=datetime.now()
            )
        """
        # Validate inputs
        if quantity <= 0:
            raise ValueError(f"quantity must be positive, got {quantity}")
        if price_usd <= 0:
            raise ValueError(f"price_usd must be positive, got {price_usd}")
        
        # Get current position
        position = self._get_position_internal(asset_id)
        
        if position is None:
            raise ValueError(f"No position exists for asset_id '{asset_id}'")
        
        if quantity > position.quantity:
            raise ValueError(
                f"Insufficient quantity: trying to sell {quantity} but only "
                f"{position.quantity} available for '{asset_id}'"
            )
        
        # Calculate realized P&L using average cost method
        cost_basis = quantity * position.avg_cost_basis_usd
        proceeds = quantity * price_usd
        realized_pnl = proceeds - cost_basis - fees_usd
        
        trade_id = str(uuid.uuid4())
        trade_value = quantity * price_usd
        now = datetime.now()
        
        with self.transaction():
            # Insert trade record with realized P&L
            self.conn.execute("""
                INSERT INTO trades (
                    trade_id, asset_id, symbol, side, quantity,
                    price_usd, trade_value_usd, executed_at,
                    fees_usd, realized_pnl_usd, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                trade_id, asset_id, symbol, SIDE_SELL, quantity,
                price_usd, trade_value, executed_at,
                fees_usd, realized_pnl, now
            ])
            
            # Update position
            self._update_position_on_sell(asset_id, quantity, now)
        
        logger.info(
            f"Recorded SELL trade {trade_id}: {quantity} {symbol} @ ${price_usd}, "
            f"realized P&L: ${realized_pnl:.2f}"
        )
        return trade_id
    
    # =========================================================================
    # Internal Position Update Methods
    # =========================================================================
    
    def _update_position_on_buy(
        self,
        asset_id: str,
        symbol: str,
        quantity: float,
        price_usd: float,
        now: datetime
    ):
        """
        Update or create position after a BUY trade.
        
        Average Cost Basis Calculation:
            new_total_cost = (old_qty * old_avg_cost) + (buy_qty * buy_price)
            new_qty = old_qty + buy_qty
            new_avg_cost = new_total_cost / new_qty
        
        :param asset_id: Asset identifier
        :param symbol: Trading symbol
        :param quantity: Quantity bought
        :param price_usd: Buy price
        :param now: Current timestamp
        
        Note: This is an internal method. Use record_buy_trade() for the public API.
        """
        existing = self._get_position_internal(asset_id)
        
        if existing is None:
            # Create new position
            self.conn.execute("""
                INSERT INTO positions (
                    asset_id, symbol, quantity, avg_cost_basis_usd,
                    market_value_usd, unrealized_pnl_usd,
                    opened_at, last_updated_at
                )
                VALUES (?, ?, ?, ?, NULL, NULL, ?, ?)
            """, [asset_id, symbol, quantity, price_usd, now, now])
        else:
            # Update existing position with new average cost
            old_total_cost = existing.quantity * existing.avg_cost_basis_usd
            new_total_cost = old_total_cost + (quantity * price_usd)
            new_quantity = existing.quantity + quantity
            new_avg_cost = new_total_cost / new_quantity
            
            self.conn.execute("""
                UPDATE positions
                SET quantity = ?,
                    avg_cost_basis_usd = ?,
                    last_updated_at = ?
                WHERE asset_id = ?
            """, [new_quantity, new_avg_cost, now, asset_id])
    
    def _update_position_on_sell(
        self,
        asset_id: str,
        quantity: float,
        now: datetime
    ):
        """
        Update position after a SELL trade.
        
        Reduces position quantity. If quantity reaches 0, the position is deleted.
        Average cost basis remains unchanged (already used in P&L calculation).
        
        :param asset_id: Asset identifier
        :param quantity: Quantity sold
        :param now: Current timestamp
        
        Note: This is an internal method. Use record_sell_trade() for the public API.
        """
        existing = self._get_position_internal(asset_id)
        
        if existing is None:
            # This shouldn't happen due to validation in record_sell_trade
            raise ValueError(f"No position found for {asset_id}")
        
        new_quantity = existing.quantity - quantity
        
        if new_quantity <= 0:
            # Close position completely
            self.conn.execute("""
                DELETE FROM positions WHERE asset_id = ?
            """, [asset_id])
            logger.info(f"Closed position for {asset_id}")
        else:
            # Reduce position
            self.conn.execute("""
                UPDATE positions
                SET quantity = ?,
                    last_updated_at = ?
                WHERE asset_id = ?
            """, [new_quantity, now, asset_id])
    
    def _get_position_internal(self, asset_id: str) -> Optional[Position]:
        """
        Get position for internal use (returns Position dataclass).
        
        :param asset_id: Asset identifier
        :return: Position dataclass or None
        """
        result = self.conn.execute("""
            SELECT asset_id, symbol, quantity, avg_cost_basis_usd,
                   market_value_usd, unrealized_pnl_usd,
                   opened_at, last_updated_at
            FROM positions
            WHERE asset_id = ?
        """, [asset_id]).fetchone()
        
        if result:
            return Position(
                asset_id=result[0],
                symbol=result[1],
                quantity=result[2],
                avg_cost_basis_usd=result[3],
                market_value_usd=result[4],
                unrealized_pnl_usd=result[5],
                opened_at=result[6],
                last_updated_at=result[7]
            )
        return None
    
    # =========================================================================
    # Read Operations (Agent-Safe Query API)
    # =========================================================================
    
    def get_open_positions(self) -> List[Dict[str, Any]]:
        """
        Get all positions with quantity > 0.
        
        This is a read-only operation with no side effects.
        Safe for exploratory agent usage.
        
        :return: List of position dictionaries, ordered by quantity descending
        
        Return format:
            [
                {
                    "asset_id": "bitcoin",
                    "symbol": "btc",
                    "quantity": 0.5,
                    "avg_cost_basis_usd": 45000.0,
                    "market_value_usd": None,
                    "unrealized_pnl_usd": None,
                    "opened_at": datetime(...),
                    "last_updated_at": datetime(...)
                },
                ...
            ]
        
        Example:
            positions = store.get_open_positions()
            for pos in positions:
                print(f"{pos['symbol']}: {pos['quantity']} @ ${pos['avg_cost_basis_usd']}")
        """
        result = self.conn.execute("""
            SELECT asset_id, symbol, quantity, avg_cost_basis_usd,
                   market_value_usd, unrealized_pnl_usd,
                   opened_at, last_updated_at
            FROM positions
            WHERE quantity > 0
            ORDER BY quantity DESC
        """).fetchall()
        
        return [
            {
                "asset_id": row[0],
                "symbol": row[1],
                "quantity": row[2],
                "avg_cost_basis_usd": row[3],
                "market_value_usd": row[4],
                "unrealized_pnl_usd": row[5],
                "opened_at": row[6],
                "last_updated_at": row[7]
            }
            for row in result
        ]
    
    def get_position(self, asset_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a single position by asset_id.
        
        This is a read-only operation with no side effects.
        Safe for exploratory agent usage.
        
        :param asset_id: CoinGecko asset identifier (e.g., "bitcoin")
        
        :return: Position dictionary or None if not found
        
        Return format:
            {
                "asset_id": "bitcoin",
                "symbol": "btc",
                "quantity": 0.5,
                "avg_cost_basis_usd": 45000.0,
                "market_value_usd": None,
                "unrealized_pnl_usd": None,
                "opened_at": datetime(...),
                "last_updated_at": datetime(...)
            }
        
        Example:
            btc_position = store.get_position("bitcoin")
            if btc_position:
                print(f"Holding {btc_position['quantity']} BTC")
            else:
                print("No BTC position")
        """
        position = self._get_position_internal(asset_id)
        
        if position:
            return {
                "asset_id": position.asset_id,
                "symbol": position.symbol,
                "quantity": position.quantity,
                "avg_cost_basis_usd": position.avg_cost_basis_usd,
                "market_value_usd": position.market_value_usd,
                "unrealized_pnl_usd": position.unrealized_pnl_usd,
                "opened_at": position.opened_at,
                "last_updated_at": position.last_updated_at
            }
        return None
    
    def get_trade_history(
        self,
        asset_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get trade history, optionally filtered by asset.
        
        This is a read-only operation with no side effects.
        Safe for exploratory agent usage.
        
        :param asset_id: Optional asset filter. If None, returns all trades.
        
        :return: List of trade dictionaries, ordered by executed_at descending
        
        Return format:
            [
                {
                    "trade_id": "uuid-string",
                    "asset_id": "bitcoin",
                    "symbol": "btc",
                    "side": "BUY",
                    "quantity": 0.5,
                    "price_usd": 45000.0,
                    "trade_value_usd": 22500.0,
                    "executed_at": datetime(...),
                    "fees_usd": 10.0,
                    "realized_pnl_usd": None,
                    "created_at": datetime(...)
                },
                ...
            ]
        
        Example:
            # Get all trades
            all_trades = store.get_trade_history()
            
            # Get trades for a specific asset
            btc_trades = store.get_trade_history(asset_id="bitcoin")
        """
        if asset_id:
            result = self.conn.execute("""
                SELECT trade_id, asset_id, symbol, side, quantity,
                       price_usd, trade_value_usd, executed_at,
                       fees_usd, realized_pnl_usd, created_at
                FROM trades
                WHERE asset_id = ?
                ORDER BY executed_at DESC
            """, [asset_id]).fetchall()
        else:
            result = self.conn.execute("""
                SELECT trade_id, asset_id, symbol, side, quantity,
                       price_usd, trade_value_usd, executed_at,
                       fees_usd, realized_pnl_usd, created_at
                FROM trades
                ORDER BY executed_at DESC
            """).fetchall()
        
        return [
            {
                "trade_id": row[0],
                "asset_id": row[1],
                "symbol": row[2],
                "side": row[3],
                "quantity": row[4],
                "price_usd": row[5],
                "trade_value_usd": row[6],
                "executed_at": row[7],
                "fees_usd": row[8],
                "realized_pnl_usd": row[9],
                "created_at": row[10]
            }
            for row in result
        ]
    
    def get_realized_pnl_summary(self) -> Dict[str, Any]:
        """
        Get aggregated realized P&L statistics.
        
        This is a read-only operation with no side effects.
        Safe for exploratory agent usage.
        
        :return: Dictionary with P&L summary statistics
        
        Return format:
            {
                "total_realized_pnl_usd": 1234.56,
                "total_trades": 50,
                "total_buys": 30,
                "total_sells": 20,
                "total_fees_usd": 50.0,
                "by_asset": {
                    "bitcoin": {
                        "realized_pnl_usd": 1000.0,
                        "trade_count": 10,
                        "total_bought_usd": 50000.0,
                        "total_sold_usd": 55000.0
                    },
                    ...
                }
            }
        
        Example:
            summary = store.get_realized_pnl_summary()
            print(f"Total P&L: ${summary['total_realized_pnl_usd']:.2f}")
            for asset, stats in summary['by_asset'].items():
                print(f"  {asset}: ${stats['realized_pnl_usd']:.2f}")
        """
        # Get total statistics
        totals = self.conn.execute("""
            SELECT 
                COALESCE(SUM(realized_pnl_usd), 0) as total_pnl,
                COUNT(*) as total_trades,
                SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) as total_buys,
                SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) as total_sells,
                COALESCE(SUM(fees_usd), 0) as total_fees
            FROM trades
        """).fetchone()
        
        # Get per-asset breakdown
        by_asset_result = self.conn.execute("""
            SELECT 
                asset_id,
                COALESCE(SUM(realized_pnl_usd), 0) as realized_pnl,
                COUNT(*) as trade_count,
                COALESCE(SUM(CASE WHEN side = 'BUY' THEN trade_value_usd ELSE 0 END), 0) as total_bought,
                COALESCE(SUM(CASE WHEN side = 'SELL' THEN trade_value_usd ELSE 0 END), 0) as total_sold
            FROM trades
            GROUP BY asset_id
            ORDER BY realized_pnl DESC
        """).fetchall()
        
        by_asset = {}
        for row in by_asset_result:
            by_asset[row[0]] = {
                "realized_pnl_usd": row[1],
                "trade_count": row[2],
                "total_bought_usd": row[3],
                "total_sold_usd": row[4]
            }
        
        return {
            "total_realized_pnl_usd": totals[0],
            "total_trades": totals[1],
            "total_buys": totals[2],
            "total_sells": totals[3],
            "total_fees_usd": totals[4],
            "by_asset": by_asset
        }
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """
        Get high-level portfolio summary.
        
        This is a read-only operation with no side effects.
        Safe for exploratory agent usage.
        
        :return: Dictionary with portfolio overview
        
        Return format:
            {
                "total_positions": 5,
                "total_cost_basis_usd": 50000.0,
                "total_realized_pnl_usd": 1234.56,
                "total_trades": 50
            }
        
        Example:
            summary = store.get_portfolio_summary()
            print(f"Positions: {summary['total_positions']}")
            print(f"Cost Basis: ${summary['total_cost_basis_usd']:.2f}")
        """
        position_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as position_count,
                COALESCE(SUM(quantity * avg_cost_basis_usd), 0) as total_cost_basis
            FROM positions
            WHERE quantity > 0
        """).fetchone()
        
        trade_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as trade_count,
                COALESCE(SUM(realized_pnl_usd), 0) as total_realized_pnl
            FROM trades
        """).fetchone()
        
        return {
            "total_positions": position_stats[0],
            "total_cost_basis_usd": position_stats[1],
            "total_realized_pnl_usd": trade_stats[1],
            "total_trades": trade_stats[0]
        }
