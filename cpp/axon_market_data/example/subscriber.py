#!/usr/bin/env python3
"""
axon_market_data ZMQ subscriber example (Python)

Connects to the market data service and prints received events.

Usage:
    python subscriber.py
    python subscriber.py tcp://localhost:5558 binance_spot.depth
    python subscriber.py tcp://localhost:5558 binance_spot.depth.ETH_USDT_SPOT
"""

import sys
import json
import zmq


def print_orderbook(data: dict) -> None:
    symbol = data["symbol"]
    seq = data["sequence"]
    bids = data["bids"][:5]
    asks = data["asks"][:5]

    print(f"  {symbol}  seq={seq}")
    print(f"  asks: {' '.join(f'{a['price']}x{a['quantity']}' for a in asks)}")
    print(f"  bids: {' '.join(f'{b['price']}x{b['quantity']}' for b in bids)}")


def print_ticker(data: dict) -> None:
    print(f"  {data['symbol']}"
          f"  bid={data['best_bid_price']}"
          f"  ask={data['best_ask_price']}"
          f"  spread={data['best_ask_price'] - data['best_bid_price']:.6f}")


def print_kline(data: dict) -> None:
    print(f"  {data['symbol']} {data['interval']}"
          f"  O={data['open']} H={data['high']} L={data['low']} C={data['close']}"
          f"  vol={data['volume']}"
          f"  {'[CLOSED]' if data['is_closed'] else ''}")


def main():
    address = sys.argv[1] if len(sys.argv) > 1 else "tcp://localhost:5558"
    topic_filter = sys.argv[2] if len(sys.argv) > 2 else ""

    print(f"Connecting to {address}")
    print(f"Topic filter: {topic_filter or '(all)'}")
    print("---")

    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(address)
    sub.setsockopt_string(zmq.SUBSCRIBE, topic_filter)

    count = 0
    while True:
        topic = sub.recv_string()
        payload = sub.recv_string()

        count += 1
        print(f"[{count}] {topic}")

        try:
            data = json.loads(payload)

            if ".depth." in topic:
                print_orderbook(data)
            elif ".ticker." in topic:
                print_ticker(data)
            elif ".kline." in topic:
                print_kline(data)
            else:
                print(f"  {payload[:200]}")
        except Exception as e:
            print(f"  parse error: {e}")
            print(f"  raw: {payload[:200]}")

        print()


if __name__ == "__main__":
    main()
