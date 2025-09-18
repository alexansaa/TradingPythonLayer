from __future__ import annotations
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import time
import math
import pandas as pd


from .config import settings
from .db import make_engine, ensure_schema_and_table, get_latest_date, upsert_bar
from .tiingo_client import tiingo_client

_engine = None
_last_run_utc: datetime | None = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = make_engine()
        ensure_schema_and_table(_engine)
    return _engine

# def get_engine() -> Engine:
#     engine = create_engine(SQLALCHEMY_URL, fast_executemany=True, pool_pre_ping=True)
#     ensure_schema_and_table(engine)  # now safe
#     return engine

def _parse_symbols(raw: str) -> List[str]:
    return [s.strip().upper() for s in raw.split(',') if s.strip()]

def _iso_to_date(iso: str) -> date:
    return datetime.fromisoformat(iso).date()

def _next_day(iso: str) -> str:
    return (_iso_to_date(iso) + timedelta(days=1)).isoformat()

def fetch_prices_for_symbol(symbol: str) -> int:
    print("into fetch_prices_for_symbol")
    engine = get_engine()
    latest = get_latest_date(engine, symbol, settings.SOURCE_EOD)

    start_iso = _next_day(latest) if latest else settings.INIT_START_DATE
    print("start_iso: ", start_iso)

    end_iso = date.today().isoformat()
    print("end_iso: ", end_iso)

    # Nothing to do
    if start_iso > end_iso:
        print("nothing to do returning")
        return 0
    
    print("about to get_dataframe")
    df: pd.DataFrame = tiingo_client.get_dataframe(
        symbol,
        startDate=start_iso,
        endDate=end_iso,
        frequency="daily",
    )
    print("end get_dataframe")

    if df is None or df.empty:
        print("empty dataframe returning")
        return 0
    
    # Normalize columns
    for col in ("open","high","low","close","volume","adjClose"):
        if col not in df.columns:
            df[col] = pd.NA

    print(df)

    # Index is datetime; normalize to date string YYYY-MM-DD
    count = 0
    for idx, row in df.iterrows():
        # idx might be Timestamp
        bar_date = (idx.date() if hasattr(idx, 'date') else pd.to_datetime(idx).date()).isoformat()
        payload = {
            "Symbol": symbol,
            "Source": settings.SOURCE_EOD,
            "BarDate": bar_date,
            "Open": None if pd.isna(row["open"]) else float(row["open"]),
            "High": None if pd.isna(row["high"]) else float(row["high"]),
            "Low": None if pd.isna(row["low"]) else float(row["low"]),
            "Close": None if pd.isna(row["close"]) else float(row["close"]),
            "Volume": None if pd.isna(row["volume"]) else int(row["volume"]),
            "AdjClose": None if pd.isna(row["adjClose"]) else float(row["adjClose"]) if not pd.isna(row["adjClose"]) else (None if pd.isna(row["close"]) else float(row["close"]))
        }
        upsert_bar(engine, payload)
        count += 1
    print("upsert_bar count: ", count)
    return count

def run_ingest_once() -> Dict[str, Any]:
    print("about to run_ingest_once")
    global _last_run_utc
    print("last_run_utc: ", _last_run_utc)
    # symbols = _parse_symbols(settings.SYMBOLS)
    symbols = settings.SYMBOLS
    print("symbols: ", symbols)
    totals = {}
    for i, sym in enumerate(symbols):
        inserted = fetch_prices_for_symbol(sym)
        totals[sym] = inserted
        # Basic rate limit pacing
        if i < len(symbols) - 1 and settings.RATE_LIMIT_SLEEP  > 0:
            time.sleep(settings.RATE_LIMIT_SLEEP )
    _last_run_utc = datetime.utcnow()
    return {"inserted": totals, "run_utc": _last_run_utc.isoformat() + "Z"}

def last_run_utc() -> str | None:
    return None if _last_run_utc is None else _last_run_utc.isoformat() + "Z"