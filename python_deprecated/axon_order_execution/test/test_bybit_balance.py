"""
Bybit account balance test - standalone, no project dependency.
Uses REST API v5 with HMAC-SHA256 authentication.
"""

import hashlib
import hmac
import time
import requests

# --- Config ---
API_KEY = "DuiQHbvp11EknZ4SzY"
API_SECRET = "eS14JlxCWZ9Q8aStMNo9KZiqcuHB8QZnn8VI"
# BASE_URL = "https://api-testnet.bybit.com"  # testnet
BASE_URL = "https://api.bybit.com"  # production
ACCOUNT_TYPE = "UNIFIED"  # UNIFIED / CONTRACT / SPOT


def sign_request(api_key: str, api_secret: str, params: str, timestamp: str, recv_window: str) -> str:
    payload = f"{timestamp}{api_key}{recv_window}{params}"
    return hmac.new(
        api_secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def get_wallet_balance(account_type: str) -> dict:
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    params = f"accountType={account_type}"

    signature = sign_request(API_KEY, API_SECRET, params, timestamp, recv_window)

    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
    }

    resp = requests.get(
        f"{BASE_URL}/v5/account/wallet-balance",
        params={"accountType": account_type},
        headers=headers,
    )
    resp.raise_for_status()
    data = resp.json()
    if data["retCode"] != 0:
        raise RuntimeError(f"API error [{data['retCode']}]: {data['retMsg']}")
    return data["result"]


if __name__ == "__main__":
    print("=== Bybit Balance Test ===")
    print(f"Endpoint: {BASE_URL}")
    print(f"Account:  {ACCOUNT_TYPE}")

    result = get_wallet_balance(ACCOUNT_TYPE)

    for account in result.get("list", []):
        print(f"\nAccount Type: {account['accountType']}")
        print(f"Total Equity:       {account.get('totalEquity', 'N/A')}")
        print(f"Total Wallet:       {account.get('totalWalletBalance', 'N/A')}")
        print(f"Total Available:    {account.get('totalAvailableBalance', 'N/A')}")
        print(f"Total Margin:       {account.get('totalMarginBalance', 'N/A')}")
        print(f"Unrealized PnL:     {account.get('totalPerpUPL', 'N/A')}")

        for coin in account.get("coin", []):
            if float(coin.get("walletBalance", 0)) > 0:
                print(f"\n  [{coin['coin']}]")
                print(f"    Wallet Balance:    {coin['walletBalance']}")
                print(f"    Available:         {coin.get('availableToWithdraw', 'N/A')}")
                print(f"    Unrealized PnL:    {coin.get('unrealisedPnl', 'N/A')}")
