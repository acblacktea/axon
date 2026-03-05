"""Protocol type definitions shared between engine and client."""

from enum import Enum


class CommandType(str, Enum):
    """Command types sent from strategy client to engine."""

    PLACE_ORDER = "place_order"
    CANCEL_ORDER = "cancel_order"
    MODIFY_ORDER = "modify_order"
    GET_ORDER = "get_order"
    GET_ALL_ORDERS = "get_all_orders"
    GET_ACTIVE_ORDERS = "get_active_orders"
    GET_TICKER = "get_ticker"
    GET_ACCOUNT_SUMMARY = "get_account_summary"
    GET_POSITIONS = "get_positions"


class EventType(str, Enum):
    """Event types published from engine to strategy clients."""

    ORDER_UPDATE = "order_update"
    ACCOUNT_UPDATE = "account_update"
    POSITION_UPDATE = "position_update"
