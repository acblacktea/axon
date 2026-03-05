"""Calais Execution Service - unified interface for strategies."""

import asyncio
import uuid
from typing import Callable

from calais_order_execution.config import Config
from calais_order_execution.ems.ems_service import EMSService
from calais_order_execution.models import Order, OrderRequest, OrderSide, OrderStatus, OrderType, Ticker
from calais_order_execution.models.portfolio import AccountSummary, Position
from calais_order_execution.oms.oms_service import OMSService
from calais_order_execution.repository import OrderRepository
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class CalaisExecutionService:
    def __init__(
        self,
        config: Config,
        repository: OrderRepository | None = None,
    ):
        """Initialize execution service.

        Args:
            config: Service configuration.
            repository: Order repository. Uses InMemoryOrderRepository if not provided.
        """
        self._config = config
        self._ems = EMSService(config)
        self._oms = OMSService(config, self._ems, repository)
        self._running = False

    async def start(self) -> None:
        """Start all components (EMS, OMS)."""
        logger.info("Starting Calais Execution Service")
        await self._ems.start()
        await self._oms.start()
        self._running = True
        logger.info("Calais Execution Service started")

    async def stop(self) -> None:
        """Stop all components and cleanup."""
        logger.info("Stopping Calais Execution Service")
        self._running = False
        await self._oms.stop()
        await self._ems.stop()
        logger.info("Calais Execution Service stopped")

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._running

    # ============= Strategy Interface =============

    async def place_order(self, exchange: str, request: OrderRequest) -> Order:
        """Place an order on the specified exchange.

        Args:
            exchange: Exchange name (e.g., "deribit").
            request: Order request details.

        Returns:
            Created order with order_id.

        Raises:
            ValueError: If exchange is not supported.
            Exception: If order placement fails.
        """
        order = await self._ems.place_order(exchange, request)
        if request.strategy_id:
            order.strategy_id = request.strategy_id
        await self._oms.add_order(order)
        return order

    async def cancel_order(self, exchange: str, order_id: str) -> bool:
        """Cancel an order.

        Args:
            exchange: Exchange name.
            order_id: Order ID to cancel.

        Returns:
            True if cancellation was successful.
        """
        return await self._ems.cancel_order(exchange, order_id)

    async def get_order(self, order_id: str) -> Order | None:
        """Get order by ID from local cache."""
        return await self._oms.get_order(order_id)

    async def get_all_orders(self) -> list[Order]:
        """Get all orders from local cache."""
        return await self._oms.get_all_orders()

    async def get_active_orders(self) -> list[Order]:
        """Get all active orders from local cache."""
        return await self._oms.get_active_orders()

    def register_order_update_callback(self, callback: Callable[[Order], None]) -> None:
        """Register a callback for order updates."""
        self._oms.register_order_update_callback(callback)

    def unregister_order_update_callback(self, callback: Callable[[Order], None]) -> None:
        """Unregister an order update callback."""
        self._oms.unregister_order_update_callback(callback)

    async def modify_order(
        self,
        exchange: str,
        order_id: str,
        amount: float | None = None,
        price: float | None = None,
    ) -> Order:
        """Modify an existing order."""
        return await self._ems.modify_order(exchange, order_id, amount=amount, price=price)

    @property
    def ems(self):
        """Access EMS service."""
        return self._ems

    @property
    def oms(self):
        """Access OMS service."""
        return self._oms

    # ============= Portfolio Interface =============

    def get_account_summary(self, exchange: str, currency: str = "BTC") -> AccountSummary | None:
        """Get account summary from local cache."""
        return self._oms.get_account(currency)

    def get_positions(self, exchange: str, currency: str | None = None) -> list[Position]:
        """Get positions from local cache."""
        positions = self._oms.get_all_positions()
        if currency:
            positions = [p for p in positions if p.instrument.startswith(currency)]
        return positions

    def register_account_update_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Register a callback for account summary updates."""
        self._oms.register_account_update_callback(callback)

    def unregister_account_update_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Unregister an account summary callback."""
        self._oms.unregister_account_update_callback(callback)

    def register_position_update_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Register a callback for position updates."""
        self._oms.register_position_update_callback(callback)

    def unregister_position_update_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Unregister a position callback."""
        self._oms.unregister_position_update_callback(callback)

    # ============= Hedge Algorithms =============

    async def get_ticker(self, exchange: str, instrument: str) -> Ticker:
        """Get ticker data for an instrument."""
        return await self._ems.get_ticker(exchange, instrument)

    async def _chase_maker_fill(
        self,
        exchange: str,
        instrument: str,
        side: OrderSide,
        amount: float,
        internal_order_id: str,
        max_attempt_second: float = 60,
        poll_interval: float = 0.2,
    ) -> float:
        """Chase maker fill by continuously placing limit orders at best price.

        If time runs out and order is not fully filled, remaining amount will be
        filled with a taker (market) order.

        Returns:
            Total filled amount.
        """
        import time

        current_order: Order | None = None
        start_time = time.time()

        while time.time() - start_time < max_attempt_second:
            ticker = await self._ems.get_ticker(exchange, instrument)
            if side == OrderSide.BUY:
                #price = ticker.best_bid_price
                price = ticker.best_ask_price
            else:
                #price = ticker.best_ask_price
                price = ticker.best_bid_price

            if price <= 0:
                logger.warning(f"Invalid price {price} for {instrument}, retrying...")
                await asyncio.sleep(poll_interval)
                continue

            # Place order if none exists
            if current_order is None:
                try:
                    request = OrderRequest(
                        instrument=instrument,
                        side=side,
                        amount=amount,
                        order_type=OrderType.LIMIT,
                        price=price,
                        #post_only=True,
                        #reject_post_only=True,
                        internal_order_id=internal_order_id,
                    )
                    current_order = await self._ems.place_order(exchange, request)
                    await self._oms.add_order(current_order)
                    logger.info(
                        f"Placed maker order {current_order.order_id}: "
                        f"{side.value} {amount} {instrument} @ {price}"
                    )
                except Exception as e:
                    logger.warning(f"Order rejected: {e}, retrying...")
                    await asyncio.sleep(poll_interval)
                    continue

            # Modify order if price changed
            if current_order.price != price:
                try:
                    current_order = await self._ems.modify_order(
                        exchange, current_order.order_id, price=price
                    )
                    current_order.internal_order_id = internal_order_id
                    await self._oms.add_order(current_order)
                    logger.info(f"Modified order {current_order.order_id} to price {price}")
                except Exception as e:
                    logger.warning(f"Failed to modify order: {e}")
                    # Order may have been filled/cancelled, check status below

            # Check order status
            updated_order = await self._ems.get_order(exchange, current_order.order_id)
            if updated_order:
                current_order = updated_order
                await self._oms.add_order(current_order)

                if current_order.status == OrderStatus.FILLED:
                    logger.info(f"Maker order filled: {current_order.order_id}")
                    return current_order.filled_amount
                elif current_order.status in (OrderStatus.CANCELLED, OrderStatus.REJECTED):
                    logger.info(f"Maker order {current_order.status.value}: {current_order.order_id}")
                    return current_order.filled_amount

            await asyncio.sleep(poll_interval)

        # Time reached, cancel maker order and get final status
        filled = 0.0
        if current_order:
            await self._ems.cancel_order(exchange, current_order.order_id)
            logger.info(f"Cancelled unfilled maker order: {current_order.order_id}")
            final_order = await self._ems.get_order(exchange, current_order.order_id)
            if final_order:
                current_order = final_order
                await self._oms.add_order(current_order)
            filled = current_order.filled_amount if current_order else 0.0

        # Place taker order for remaining amount
        remaining = amount - filled
        if remaining > 1e-12:
            logger.info(f"Maker chase timeout, placing taker order for remaining {remaining}")
            try:
                taker_request = OrderRequest(
                    instrument=instrument,
                    side=side,
                    amount=remaining,
                    order_type=OrderType.MARKET,
                    internal_order_id=internal_order_id,
                )
                taker_order = await self._ems.place_order(exchange, taker_request)
                await self._oms.add_order(taker_order)
                logger.info(f"Taker order placed: {taker_order.order_id}")
                filled += remaining
            except Exception as e:
                logger.error(f"Failed to place taker order for remaining amount: {e}")

        logger.info(f"Maker chase ended: filled {filled}/{amount} after {max_attempt_second}s")
        return filled

    def place_order_hedge_deribit_options(
        self,
        symbol1: str,
        symbol2: str,
        side1: OrderSide,
        side2: OrderSide,
        amount: float,
        batch_amount: float,
        chase_maker_max_attempt_second: float = 60,
    ) -> str:
        """Place a hedge order for Deribit options (async, non-blocking).

        Returns:
            internal_order_id: Use this to track all orders from this hedge.
        """
        exchange = "deribit"
        if not self._ems.has(exchange):
            raise ValueError("Deribit EMS not configured")

        internal_order_id = uuid.uuid4().hex

        logger.info(
            f"Starting hedge: {symbol1} (maker) / {symbol2} (taker), "
            f"total={amount}, batch={batch_amount}, internal_id={internal_order_id}"
        )

        asyncio.create_task(
            self._run_hedge_deribit_options(
                internal_order_id=internal_order_id,
                symbol1=symbol1,
                symbol2=symbol2,
                side1=side1,
                side2=side2,
                amount=amount,
                batch_amount=batch_amount,
                chase_maker_max_attempt_second=chase_maker_max_attempt_second,
            )
        )

        return internal_order_id

    async def _run_hedge_deribit_options(
        self,
        internal_order_id: str,
        symbol1: str,
        symbol2: str,
        side1: OrderSide,
        side2: OrderSide,
        amount: float,
        batch_amount: float,
        chase_maker_max_attempt_second: float,
    ) -> None:
        """Run the hedge logic (background task).

        Guaranteed to fill all amount since _chase_maker_fill will use taker
        orders for any remaining amount after timeout.
        """
        exchange = "deribit"
        remaining_amount = amount

        try:
            while remaining_amount > 1e-12:
                current_batch = min(batch_amount, remaining_amount)

                logger.info(f"Batch: {side1.value} {current_batch} {symbol1} (maker)")
                maker_filled = await self._chase_maker_fill(
                    exchange=exchange,
                    instrument=symbol1,
                    side=side1,
                    amount=current_batch,
                    internal_order_id=internal_order_id,
                    max_attempt_second=chase_maker_max_attempt_second,
                )

                if maker_filled > 0:
                    logger.info(f"Batch: {side2.value} {maker_filled} {symbol2} (taker)")
                    taker_request = OrderRequest(
                        instrument=symbol2,
                        side=side2,
                        amount=maker_filled,
                        order_type=OrderType.MARKET,
                        internal_order_id=internal_order_id,
                    )
                    taker_order = await self._ems.place_order(exchange, taker_request)
                    await self._oms.add_order(taker_order)
                    logger.info(f"Taker order placed: {taker_order.order_id}")

                remaining_amount -= maker_filled
                logger.info(f"Batch complete. Filled: {maker_filled}, Remaining: {remaining_amount}")

            logger.info(f"Hedge {internal_order_id} complete")

        except Exception as e:
            logger.error(f"Hedge {internal_order_id} failed: {e}")

    # ============= Context Manager =============

    async def __aenter__(self) -> "CalaisExecutionService":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
