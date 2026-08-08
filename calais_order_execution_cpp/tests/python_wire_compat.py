#!/usr/bin/env python3
"""Cross-implementation wire compatibility check.

Runs the C++ fixture tool, then feeds every object it produced through the
*real* Python implementation (transport/serialization.py) and asserts that
Python, given the C++ bytes, reconstructs the dataclass and re-serialises it to
the same thing.

That is the property that actually matters: an existing Python StrategyClient
must be able to talk to the C++ engine without modification.

For each fixture we check three increasingly strict things:

  1. Python can deserialize() the C++ JSON into the dataclass at all.
  2. The key set and ORDER match the dataclass field declaration order -- i.e.
     the C++ side emits every field Python emits, no more and no fewer.
  3. Re-serialising the reconstructed object reproduces the C++ bytes exactly,
     modulo json.dumps' separator defaults.

Check 3 is compared against json.dumps(..., separators=(',', ':')) because
Python defaults to ", " / ": " while nlohmann emits compact. That is a
whitespace difference in the encoder, not a difference in the data; see the
note at the top of include/calais/transport/wire.h.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import subprocess
import sys
from typing import Any


def load_python_types(project_root: str) -> dict[str, Any]:
    sys.path.insert(0, project_root)
    try:
        from calais_order_execution.models.fill import Fill
        from calais_order_execution.models.messages import Command, Event, Response
        from calais_order_execution.models.order import Order, OrderRequest, Ticker
        from calais_order_execution.models.portfolio import AccountSummary, Position
        from calais_order_execution.transport import serialization
    except ImportError as exc:  # pragma: no cover - environment problem
        print(f"SKIP: cannot import the Python implementation: {exc}")
        raise SystemExit(0)

    return {
        "classes": {
            "Order": Order,
            "OrderRequest": OrderRequest,
            "Ticker": Ticker,
            "Fill": Fill,
            "AccountSummary": AccountSummary,
            "Position": Position,
            "Command": Command,
            "Response": Response,
            "Event": Event,
        },
        "serialization": serialization,
    }


def compact(obj: Any) -> str:
    """Match nlohmann's dump(): no spaces after separators."""
    return json.dumps(obj, separators=(",", ":"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture-tool", required=True)
    parser.add_argument("--python-project", required=True)
    args = parser.parse_args()

    env = load_python_types(args.python_project)
    classes = env["classes"]
    serialization = env["serialization"]

    result = subprocess.run(
        [args.fixture_tool], capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        print(f"FAIL: fixture tool exited {result.returncode}")
        print(result.stderr)
        return 1

    lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
    if not lines:
        print("FAIL: fixture tool produced no output")
        return 1

    failures: list[str] = []
    checked = 0

    for lineno, line in enumerate(lines, start=1):
        record = json.loads(line)
        kind = record["kind"]
        cpp_bytes = record["bytes"]
        cls = classes[kind]

        def fail(msg: str) -> None:
            failures.append(f"[line {lineno}] {kind}: {msg}\n    C++: {cpp_bytes}")

        # --- 1. Python can parse and deserialize what C++ emitted -------------
        try:
            parsed = json.loads(cpp_bytes)
        except json.JSONDecodeError as exc:
            fail(f"C++ emitted invalid JSON: {exc}")
            continue

        try:
            obj = serialization.deserialize(cls, parsed)
        except Exception as exc:  # noqa: BLE001 - any failure is a failure
            fail(f"Python deserialize() raised {type(exc).__name__}: {exc}")
            continue

        # --- 2. Same fields, same order --------------------------------------
        expected_keys = [f.name for f in dataclasses.fields(cls)]
        actual_keys = list(parsed.keys())
        if actual_keys != expected_keys:
            missing = [k for k in expected_keys if k not in actual_keys]
            extra = [k for k in actual_keys if k not in expected_keys]
            detail = f"key order mismatch\n    expected: {expected_keys}\n    actual:   {actual_keys}"
            if missing:
                detail += f"\n    missing:  {missing}"
            if extra:
                detail += f"\n    extra:    {extra}"
            fail(detail)
            continue

        # --- 3. Round-trip reproduces the same bytes -------------------------
        round_tripped = serialization.serialize(obj)
        python_bytes = compact(round_tripped)
        if python_bytes != cpp_bytes:
            diff_at = next(
                (
                    i
                    for i, (a, b) in enumerate(zip(python_bytes, cpp_bytes))
                    if a != b
                ),
                min(len(python_bytes), len(cpp_bytes)),
            )
            fail(
                "round-trip mismatch\n"
                f"    python: {python_bytes}\n"
                f"    first difference at offset {diff_at}: "
                f"python={python_bytes[diff_at:diff_at + 24]!r} "
                f"cpp={cpp_bytes[diff_at:diff_at + 24]!r}"
            )
            continue

        checked += 1

    if failures:
        print(f"FAIL: {len(failures)} of {len(lines)} fixtures diverged\n")
        for f in failures:
            print(f)
            print()
        return 1

    print(f"OK: {checked} fixtures round-trip identically through Python")
    return 0


if __name__ == "__main__":
    sys.exit(main())
