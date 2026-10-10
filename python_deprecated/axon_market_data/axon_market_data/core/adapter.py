"""
Abstract Exchange Adapter - Base class for all exchange implementations
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Callable, Awaitable, Set, Dict, Any
from datetime import datetime
import asyncio
import logging
import time

from .models import Instrument, Orderbook, OrderbookSnapshot, DataType, MarketDataEvent
from ..utils import metrics


# Type alias for callback
MarketDataCallback = Callable[[MarketDataEvent], Awaitable[None]]


class ExchangeAdapter(ABC):
    """
    Abstract base class for exchange adapters.
    All exchange-specific implementations should inherit from this class.
    """

    # Declare supported data types (override in subclasses)
    SUPPORTED_DATA_TYPES: Set[DataType] = {DataType.ORDERBOOK}

    def __init__(self, exchange_name: str, logger: Optional[logging.Logger] = None):
        self.exchange_name = exchange_name
        self.logger = logger or logging.getLogger(f"mds.{exchange_name}")
        # Callbacks organized by data type
        self._callbacks_by_type: Dict[DataType, List[MarketDataCallback]] = {
            dt: [] for dt in DataType
        }
        self._subscribed_symbols: Set[str] = set()
        self._is_connected: bool = False
        self._is_running: bool = False
        # Symbol mapping: unified -> exchange, exchange -> unified
        self._symbol_map_to_exchange: Dict[str, str] = {}
        self._symbol_map_to_unified: Dict[str, str] = {}
    
    @property
    def is_connected(self) -> bool:
        return self._is_connected
    
    @property
    def subscribed_symbols(self) -> Set[str]:
        return self._subscribed_symbols.copy()

    # ==================== Symbol Conversion ====================

    def convert_to_exchange_symbol(self, unified_symbol: str) -> str:
        """
        Convert unified symbol (BTC_USDT) to exchange-specific format.
        Override in subclasses for exchange-specific conversion.
        """
        return unified_symbol

    def convert_to_unified_symbol(self, exchange_symbol: str) -> str:
        """
        Convert exchange-specific symbol to unified format (BTC_USDT).
        Override in subclasses for exchange-specific conversion.
        """
        return exchange_symbol

    def convert_symbols(self, unified_symbols: List[str]) -> List[str]:
        """Convert list of unified symbols and maintain mapping."""
        exchange_symbols = []
        for unified in unified_symbols:
            exchange = self.convert_to_exchange_symbol(unified)
            self._symbol_map_to_exchange[unified] = exchange
            self._symbol_map_to_unified[exchange] = unified
            exchange_symbols.append(exchange)
        return exchange_symbols

    def get_unified_symbol(self, exchange_symbol: str) -> str:
        """Get unified symbol from exchange symbol."""
        if exchange_symbol in self._symbol_map_to_unified:
            return self._symbol_map_to_unified[exchange_symbol]
        return self.convert_to_unified_symbol(exchange_symbol)

    # ==================== Abstract Methods ====================
    
    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to the exchange.
        Should handle authentication if required.
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """
        Close connection to the exchange.
        Should cleanup all resources.
        """
        pass
    
    @abstractmethod
    async def subscribe(
        self,
        data_type: DataType,
        symbols: List[str],
        **kwargs
    ) -> None:
        """
        Subscribe to market data updates for given symbols.

        Args:
            data_type: Type of data to subscribe (ORDERBOOK, INDEX_PRICE, etc.)
            symbols: List of symbols to subscribe
            **kwargs: Data type specific parameters
                - depth (int): For orderbook subscriptions (default: 10000)
        """
        pass

    @abstractmethod
    async def unsubscribe(self, data_type: DataType, symbols: List[str]) -> None:
        """
        Unsubscribe from market data updates for given symbols.

        Args:
            data_type: Type of data to unsubscribe
            symbols: List of symbols to unsubscribe
        """
        pass
    
    @abstractmethod
    async def get_instruments(
        self, 
        currency: Optional[str] = None,
        kind: Optional[str] = None,
        expired: bool = False
    ) -> List[Instrument]:
        """
        Get available instruments from exchange.
        
        Args:
            currency: Filter by base currency (e.g., 'BTC')
            kind: Filter by instrument type (e.g., 'option', 'future')
            expired: Include expired instruments
            
        Returns:
            List of available instruments
        """
        pass
    
    @abstractmethod
    async def request_orderbook_snapshot(self, symbol: str, depth: int = 10000) -> Orderbook:
        """
        Request a full orderbook snapshot for a symbol.
        Used for initial sync and recovery.
        
        Args:
            symbol: Symbol to get snapshot for
            depth: Orderbook depth
            
        Returns:
            Full orderbook snapshot
        """
        pass
    
    # ==================== Callback System ====================

    def supports_data_type(self, data_type: DataType) -> bool:
        """Check if this adapter supports a specific data type"""
        return data_type in self.SUPPORTED_DATA_TYPES

    def add_callback_for_type(
        self,
        data_type: DataType,
        callback: MarketDataCallback
    ) -> None:
        """Register a callback for a specific data type"""
        if callback not in self._callbacks_by_type[data_type]:
            self._callbacks_by_type[data_type].append(callback)

    def remove_callback_for_type(
        self,
        data_type: DataType,
        callback: MarketDataCallback
    ) -> None:
        """Remove a callback for a specific data type"""
        if callback in self._callbacks_by_type[data_type]:
            self._callbacks_by_type[data_type].remove(callback)

    async def _notify_market_data(self, event: MarketDataEvent) -> None:
        """Notify all registered callbacks for a market data event"""
        metrics.messages_total.labels(
            exchange=self.exchange_name,
            data_type=event.data_type.value,
        ).inc()

        # Record latency if data has timestamps
        data = event.data
        if data is not None:
            ts = getattr(data, "timestamp", None)
            local_ts = getattr(data, "local_timestamp", None)
            now_ms = time.time() * 1000
            if ts:
                metrics.e2e_latency_ms.labels(
                    exchange=self.exchange_name,
                    data_type=event.data_type.value,
                ).observe(now_ms - ts)
                if local_ts:
                    metrics.network_latency_ms.labels(
                        exchange=self.exchange_name,
                        data_type=event.data_type.value,
                    ).observe(local_ts - ts)

        for callback in self._callbacks_by_type[event.data_type]:
            try:
                await callback(event)
            except Exception as e:
                metrics.errors_total.labels(
                    exchange=self.exchange_name, category="callback"
                ).inc()
                self.logger.error(f"Callback error for {event.data_type}: {e}", exc_info=True)

    async def _emit_market_data(
        self,
        data_type: DataType,
        event_type: str,
        symbol: str,
        data: Any = None,
        error: Optional[str] = None
    ) -> None:
        """Emit a market data event with unified symbol"""
        # Convert exchange symbol to unified format
        unified_symbol = self.get_unified_symbol(symbol)
        event = MarketDataEvent(
            data_type=data_type,
            event_type=event_type,
            symbol=unified_symbol,
            exchange=self.exchange_name,
            data=data,
            error=error
        )
        await self._notify_market_data(event)

    # ==================== Orderbook Emit Helpers ====================

    async def _emit_snapshot(self, symbol: str, snapshot: OrderbookSnapshot) -> None:
        """Emit an orderbook snapshot event"""
        await self._emit_market_data(DataType.ORDERBOOK, "snapshot", symbol, snapshot)

    async def _emit_update(self, symbol: str, snapshot: OrderbookSnapshot) -> None:
        """Emit an orderbook update event"""
        await self._emit_market_data(DataType.ORDERBOOK, "update", symbol, snapshot)

    async def _emit_error(self, symbol: str, error: str) -> None:
        """Emit an orderbook error event"""
        await self._emit_market_data(DataType.ORDERBOOK, "error", symbol, error=error)

    async def _emit_reconnect(self, symbol: str) -> None:
        """Emit an orderbook reconnect event"""
        await self._emit_market_data(DataType.ORDERBOOK, "reconnect", symbol)


class OrderbookManager:
    """
    Manages local orderbook state for multiple symbols.
    Handles sequence validation and recovery.
    """

    def __init__(
        self,
        adapter: ExchangeAdapter,
        logger: Optional[logging.Logger] = None,
        max_recovery_attempts: int = 10,
        recovery_delay: float = 1.0,
        update_buffer_size: int = 1000
    ):
        self.adapter = adapter
        self.logger = logger or logging.getLogger(f"mds.{adapter.exchange_name}.manager")
        self.max_recovery_attempts = max_recovery_attempts
        self.recovery_delay = recovery_delay
        self.update_buffer_size = update_buffer_size

        # Local orderbook storage
        self._orderbooks: dict[str, Orderbook] = {}

        # Recovery state
        self._recovering: Set[str] = set()
        self._recovery_attempts: dict[str, int] = {}

        # Update buffer for recovery - stores updates during recovery
        # Key: symbol, Value: list of (sequence, data) tuples
        self._update_buffers: dict[str, List[tuple[int, dict]]] = {}

        # Lock for thread safety
        self._locks: dict[str, asyncio.Lock] = {}
    
    def _get_lock(self, symbol: str) -> asyncio.Lock:
        """Get or create a lock for a symbol"""
        if symbol not in self._locks:
            self._locks[symbol] = asyncio.Lock()
        return self._locks[symbol]
    
    def get_orderbook(self, symbol: str) -> Optional[Orderbook]:
        """Get current orderbook for a symbol"""
        return self._orderbooks.get(symbol)
    
    def get_snapshot(self, symbol: str, depth: int = 10000) -> Optional[OrderbookSnapshot]:
        """Get orderbook snapshot for a symbol"""
        ob = self._orderbooks.get(symbol)
        if ob:
            return ob.to_snapshot(depth)
        return None
    
    def get_all_snapshots(self, depth: int = 10000) -> dict[str, OrderbookSnapshot]:
        """Get all orderbook snapshots"""
        return {
            symbol: ob.to_snapshot(depth)
            for symbol, ob in self._orderbooks.items()
        }
    
    async def apply_snapshot(self, symbol: str, data: dict) -> None:
        """
        Apply a full orderbook snapshot.
        
        Args:
            symbol: Symbol to update
            data: Snapshot data with bids, asks, sequence, timestamp
        """
        async with self._get_lock(symbol):
            if symbol not in self._orderbooks:
                self._orderbooks[symbol] = Orderbook(
                    symbol=symbol,
                    exchange=self.adapter.exchange_name
                )
            
            ob = self._orderbooks[symbol]
            ob.clear()
            
            # Apply bids and asks
            if 'bids' in data:
                ob.update_bids(data['bids'])
            if 'asks' in data:
                ob.update_asks(data['asks'])
            
            ob.sequence = data.get('sequence') or data.get('change_id')
            ob.prev_sequence = data.get('prev_change_id')
            ob.timestamp = data.get('timestamp')
            ob.local_timestamp = data.get('local_timestamp') or int(time.time() * 1000)
            ob.is_snapshot = True
            
            self.logger.debug(f"Applied snapshot for {symbol}, seq={ob.sequence}")
            
            # Clear recovery state
            self._recovering.discard(symbol)
            self._recovery_attempts.pop(symbol, None)
    
    async def apply_update(self, symbol: str, data: dict) -> bool:
        """
        Apply an incremental orderbook update.
        Returns True if update was applied successfully, False if recovery needed.

        During recovery, updates are buffered and will be replayed after snapshot.

        Args:
            symbol: Symbol to update
            data: Update data with bids, asks, sequence, prev_sequence

        Returns:
            True if update applied, False if sequence gap detected
        """
        async with self._get_lock(symbol):
            new_sequence = data.get('sequence') or data.get('change_id')
            prev_sequence = data.get('prev_change_id')

            # If recovering, buffer the update for later replay
            if symbol in self._recovering:
                self._buffer_update(symbol, new_sequence, data)
                return True  # Return True to indicate message was handled

            if symbol not in self._orderbooks:
                self.logger.warning(f"No orderbook for {symbol}, requesting snapshot")
                return False

            ob = self._orderbooks[symbol]

            # Skip updates that are older than current state
            if ob.sequence is not None and new_sequence is not None:
                if new_sequence <= ob.sequence:
                    self.logger.debug(
                        f"Skipping stale update for {symbol}: "
                        f"update_seq={new_sequence}, current_seq={ob.sequence}"
                    )
                    return True  # Not an error, just stale data

            # Sequence validation
            if ob.sequence is not None and prev_sequence is not None:
                if prev_sequence != ob.sequence:
                    self.logger.warning(
                        f"Sequence gap for {symbol}: expected {ob.sequence}, "
                        f"got prev_change_id={prev_sequence}"
                    )
                    return False

            # Apply updates
            if 'bids' in data:
                ob.update_bids(data['bids'])
            if 'asks' in data:
                ob.update_asks(data['asks'])

            ob.prev_sequence = ob.sequence
            ob.sequence = new_sequence
            ob.timestamp = data.get('timestamp')
            ob.local_timestamp = data.get('local_timestamp') or int(time.time() * 1000)
            ob.is_snapshot = False

            return True

    def _buffer_update(self, symbol: str, sequence: int, data: dict) -> None:
        """Buffer an update during recovery"""
        if symbol not in self._update_buffers:
            self._update_buffers[symbol] = []

        buffer = self._update_buffers[symbol]
        buffer.append((sequence, data.copy()))

        # Limit buffer size to prevent memory issues
        if len(buffer) > self.update_buffer_size:
            # Remove oldest entries
            buffer[:] = buffer[-self.update_buffer_size:]
            self.logger.warning(
                f"Update buffer for {symbol} exceeded limit, "
                f"dropped oldest updates"
            )
    
    async def trigger_recovery(self, symbol: str) -> None:
        """
        Trigger orderbook recovery for a symbol.
        Requests a new snapshot and replays buffered updates.

        Recovery process:
        1. Mark symbol as recovering (updates will be buffered)
        2. Request snapshot from exchange
        3. Apply snapshot
        4. Replay buffered updates that are newer than snapshot
        5. Clear recovery state
        """
        if symbol in self._recovering:
            self.logger.debug(f"Already recovering {symbol}")
            return

        self._recovering.add(symbol)
        # Initialize update buffer for this symbol
        self._update_buffers[symbol] = []

        attempts = self._recovery_attempts.get(symbol, 0)

        if attempts >= self.max_recovery_attempts:
            self.logger.error(
                f"Max recovery attempts reached for {symbol}, giving up"
            )
            self._recovering.discard(symbol)
            self._recovery_attempts.pop(symbol, None)
            self._update_buffers.pop(symbol, None)
            await self.adapter._emit_error(
                symbol,
                f"Failed to recover after {attempts} attempts"
            )
            return

        self._recovery_attempts[symbol] = attempts + 1

        try:
            self.logger.info(
                f"Recovering orderbook for {symbol} (attempt {attempts + 1})"
            )

            # Wait before recovery
            await asyncio.sleep(self.recovery_delay * (attempts + 1))

            # Request new snapshot
            snapshot = await self.adapter.request_orderbook_snapshot(symbol)
            snapshot_sequence = snapshot.sequence

            # Apply snapshot data (this clears recovery state)
            await self.apply_snapshot(symbol, {
                'bids': list(snapshot.bids.items()),
                'asks': list(snapshot.asks.items()),
                'sequence': snapshot.sequence,
                'timestamp': snapshot.timestamp
            })

            # Replay buffered updates that are newer than snapshot
            buffered_updates = self._update_buffers.pop(symbol, [])
            replayed_count = 0
            skipped_count = 0

            # Sort by sequence to ensure correct order
            buffered_updates.sort(key=lambda x: x[0] if x[0] is not None else 0)

            for seq, update_data in buffered_updates:
                if seq is None or seq <= snapshot_sequence:
                    # Skip updates older than or equal to snapshot
                    skipped_count += 1
                    continue

                prev_seq = update_data.get('prev_change_id')
                ob = self._orderbooks.get(symbol)

                if ob is None:
                    break

                # Check if this update is the next expected one
                if prev_seq is not None and ob.sequence is not None:
                    if prev_seq != ob.sequence:
                        # Gap detected during replay - need to recover again
                        self.logger.warning(
                            f"Gap during replay for {symbol}: "
                            f"expected {ob.sequence}, got prev={prev_seq}"
                        )
                        # Trigger another recovery
                        asyncio.create_task(self.trigger_recovery(symbol))
                        return

                # Apply the buffered update directly (not through apply_update to avoid lock)
                if 'bids' in update_data:
                    ob.update_bids(update_data['bids'])
                if 'asks' in update_data:
                    ob.update_asks(update_data['asks'])

                ob.prev_sequence = ob.sequence
                ob.sequence = seq
                ob.timestamp = update_data.get('timestamp')
                ob.is_snapshot = False
                replayed_count += 1

            self.logger.info(
                f"Recovery complete for {symbol}: "
                f"snapshot_seq={snapshot_sequence}, "
                f"replayed={replayed_count}, skipped={skipped_count}"
            )
            await self.adapter._emit_reconnect(symbol)

        except Exception as e:
            self.logger.error(f"Recovery failed for {symbol}: {e}", exc_info=True)
            self._recovering.discard(symbol)
            self._update_buffers.pop(symbol, None)
            # Retry
            asyncio.create_task(self.trigger_recovery(symbol))
    
    def remove_orderbook(self, symbol: str) -> None:
        """Remove orderbook for a symbol"""
        self._orderbooks.pop(symbol, None)
        self._locks.pop(symbol, None)
        self._recovering.discard(symbol)
        self._recovery_attempts.pop(symbol, None)
        self._update_buffers.pop(symbol, None)

    def clear(self) -> None:
        """Clear all orderbooks"""
        self._orderbooks.clear()
        self._locks.clear()
        self._recovering.clear()
        self._recovery_attempts.clear()
        self._update_buffers.clear()
