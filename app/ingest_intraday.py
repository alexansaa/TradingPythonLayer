from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
import requests
# import json;
from websocket import create_connection
import simplejson as json

from .config import settings
from .db import get_last_intraday_time, upsert_intraday
from .ingest import get_engine
from .usage import can_make_call, increment_calls

_TIINGO_BASE = "https://api.tiingo.com"

def _interval_seconds(resample: str) -> int:
    if resample.endswith("min"):
        return int(resample.replace("min", "")) * 60
    if resample.endswith("sec"):
        return int(resample.replace("sec", ""))
    # default 60s
    return 60

def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now() -> datetime:
    return _now_utc()

def sync_intraday_for_symbol(symbol: str, window_minutes: Optional[int] = None) -> Dict[str, Any]:
    symbol = symbol.upper()
    if not can_make_call():
        return {"symbol": symbol, "skipped": True, "reason": "rate-limit-guard"}

    isec = _interval_seconds(settings.INTRADAY_RESAMPLE)
    engine = get_engine()

    # Determine window to fetch
    last_str = get_last_intraday_time(engine, symbol, "tiingo_iex", isec)
    now = _now_utc()

    if last_str:
        # backfill 2 minutes overlap to handle late updates
        start = datetime.strptime(last_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) - timedelta(minutes=2)
    else:
        wm = window_minutes or settings.INTRADAY_WINDOW_MINUTES
        start = now - timedelta(minutes=wm)
    end = now

    params = {
        "startDate": start.date().isoformat(),
        "endDate": end.date().isoformat(),
        "resampleFreq": settings.INTRADAY_RESAMPLE,
        "columns": "open,high,low,close,volume",
    }

    # params = {
    #     "tickers": "btcusd",
    #     "startDate": "2019-01-02",
    #     "resampleFreq": "5min"
    # }

    headers = {
        'Content-Type': 'application/json',
        'Authorization' : f"Token {settings.TIINGO_API_KEY}"
        }

    #Llamama a un conjunto de datos   
    url = f"{_TIINGO_BASE}/iex/{symbol}/prices"
    # url = f"{_TIINGO_BASE}/crypto/prices"
    #hace una prueba para obtener respuesta valida
    # url = f"{_TIINGO_BASE}/api/test"
    print(url)
    r = requests.get(url, params=params, headers=headers, timeout=20)

    print(r.url)
    print(r.status_code, r.headers.get("content-type"), r.url)
    print(r.text[:1000])
    print(r.json())
    print(json.dumps(r.json(), indent=2)[:1500])
    print("finish first response")

    # ########
    # ws = create_connection("wss://api.tiingo.com/tiingo/crypto/top")
    # # ws = create_connection("wss://api.tiingo.com/test")
    # print("wss connetion created")

    # subscribe = {
    #                 'eventName':'subscribe',
    #                 'authorization':f'{settings.TIINGO_API_KEY}',
    #                 'eventData': {
    #                             'tickers':['btcusd']
    #                             }
    #                 }

    # print("ws.send")
    # ws.send(json.dumps(subscribe))
    # print("before while respnse")
    # print(ws.recv())
    # print("printing while response")
    # while True:
    #     print(ws.recv())
    ########

    r = requests.get(url, params=params, headers=headers, timeout=20)
    print(r)
    r.raise_for_status()
    rows: List[dict] = r.json() or []

    print(rows)

    inserted = 0
    for row in rows:
        # Tiingo returns ISO with Z
        ts = datetime.fromisoformat(row["date"].replace("Z", "+00:00"))
        payload = {
            "Symbol": symbol,
            "Source": "tiingo_iex",
            "BarTime": ts,
            "IntervalSec": isec,
            "Open": row.get("open"),
            "High": row.get("high"),
            "Low": row.get("low"),
            "Close": row.get("close"),
            "Volume": row.get("volume"),
        }
        upsert_intraday(engine, payload)
        inserted += 1
    increment_calls(1) # account for this API request
    return {"symbol": symbol, "inserted": inserted, "from": _iso(start), "to": _iso(end)}

def sync_intraday_for_all_symbols(window_minutes: Optional[int] = None) -> Dict[str, Any]:
    # symbols = [s.strip().upper() for s in settings.SYMBOLS.split(',') if s.strip()]
    symbols = settings.SYMBOLS
    totals: Dict[str, Any] = {}
    for sym in symbols:
        res = sync_intraday_for_symbol(sym, window_minutes)
        totals[sym] = res
    return totals

