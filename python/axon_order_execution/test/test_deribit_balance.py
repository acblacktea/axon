"""
Deribit account balance test - standalone, no project dependency.
Uses REST API with authentication.
"""

import hashlib
import hmac
import time
import requests

# --- Config ---
API_KEY = "ceMxLIv9"
API_SECRET = "ALX1msMoHttVzRHHgH2_YNvRd30fA3VS8YC3lMcEpSY"
# BASE_URL = "https://test.deribit.com"  # testnet
BASE_URL = "https://www.deribit.com"  # production
CURRENCY = "BTC"


def get_access_token() -> str:
    resp = requests.get(
        f"{BASE_URL}/api/v2/public/auth",
        params={
            "grant_type": "client_credentials",
            "client_id": API_KEY,
            "client_secret": API_SECRET,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Auth failed: {data['error']}")
    return data["result"]["access_token"]


def get_account_summary(token: str, currency: str) -> dict:
    resp = requests.get(
        f"{BASE_URL}/api/v2/private/get_account_summary",
        params={"currency": currency},
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"API error: {data['error']}")
    return data["result"]


if __name__ == "__main__":
    print("=== Deribit Balance Test ===")
    print(f"Endpoint: {BASE_URL}")

    token = get_access_token()
    print("Auth OK")

    summary = get_account_summary(token, CURRENCY)

    print(f"\nCurrency:           {summary['currency']}")
    print(f"Equity:             {summary['equity']}")
    print(f"Balance:            {summary['balance']}")
    print(f"Available Funds:    {summary['available_funds']}")
    print(f"Margin Balance:     {summary['margin_balance']}")
    print(f"Initial Margin:     {summary['initial_margin']}")
    print(f"Maintenance Margin: {summary['maintenance_margin']}")
    print(f"Unrealized PnL:     {summary.get('futures_session_upl', 'N/A')}")
