import os, time, threading, random
from datetime import datetime, timedelta, date
from typing import List, Optional

import pyodbc
from fastapi import FastAPI, HTTPException
from tiingo import TiingoClient
from apscheduler.schedulers.background import BackgroundScheduler
from dateutil import parser as dtparse

# --------------------
# Config (env-driven)
# --------------------
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "trading-sql")
SQLSERVER_DB   = os.getenv("SQLSERVER_DB",   "trading")
SQLSERVER_USER = os.getenv("SQLSERVER_USER", "sa")
SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD", "")

SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS","AAPL").split(",") if s.strip()]
INIT_START_DATE = os.getenv("INIT_START_DATE","2020-01-01")
FETCH_INTERVAL_MINUTES = int(os.getenv("FETCH_INTERVAL_MINUTES","15"))

MAX_CALLS_PER_MINUTE = int(os.getenv("MAX_CALLS_PER_MINUTE","50"))
MAX_CALLS_PER_DAY = int(os.getenv("MAX_CALLS_PER_DAY","4500"))

ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER","true").lower() == "true"

# --------------------
# Infra helpers
# --------------------
def get_conn():
    cs = (
      "DRIVER={ODBC Driver 18 for SQL Server};"
      f"SERVER={SQLSERVER_HOST},1433;"
      f"DATABASE={SQLSERVER_DB};UID={SQLSERVER_USER};PWD={SQLSERVER_PASSWORD};"
      "Encrypt=Yes;TrustServerCertificate=Yes"
    )
    return pyodbc.connect(cs, autocommit=True)

def ensure_table():
    ddl = """
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'market')
      EXEC('CREATE SCHEMA market');

    IF OBJECT_ID('market.PriceBar','U') IS NULL
    BEGIN
      CREATE TABLE market.PriceBar(
        Id BIGINT IDENTITY(1,1) PRIMARY KEY,
        Symbol NVARCHAR(32) NOT NULL,
        Source NVARCHAR(16) NOT NULL DEFAULT 'tiingo',
        BarDate DATE NOT NULL,
        [Open] DECIMAL(18,6) NULL,
        [High] DECIMAL(18,6) NULL,
        [Low]  DECIMAL(18,6) NULL,
        [Close] DECIMAL(18,6) NULL,
        Volume BIGINT NULL,
        AdjClose DECIMAL(18,6) NULL,
        CONSTRAINT UQ_PriceBar UNIQUE(Symbol, Source, BarDate)
      );
      CREATE INDEX IX_PriceBar_Symbol_BarDate ON market.PriceBar(Symbol, BarDate DESC);
    END
    """
    with get_conn() as c:
        c.cursor().execute(ddl)

def upsert_row(cur, r):
    cur.execute("""
      MERGE market.PriceBar AS tgt
      USING (SELECT ? AS Symbol, 'tiingo' AS Source, ? AS BarDate) AS src
      ON (tgt.Symbol=src.Symbol AND tgt.Source=src.Source AND tgt.BarDate=src.BarDate)
      WHEN MATCHED THEN UPDATE SET
        [Open]=?, [High]=?, [Low]=?, [Close]=?, Volume=?, AdjClose=?
      WHEN NOT MATCHED THEN INSERT(Symbol,Source,BarDate,[Open],[High],[Low],[Close],Volume,AdjClose)
        VALUES(?, 'tiingo', ?, ?, ?, ?, ?, ?, ?);
    """,
    r["symbol"], r["BarDate"],
    r["Open"], r["High"], r["Low"], r["Close"], r["Volume"], r["AdjClose"],
    r["symbol"], r["BarDate"],
    r["Open"], r["High"], r["Low"], r["Close"], r["Volume"], r["AdjClose"])

def max_bar_date(cur, symbol)->Optional[date]:
    cur.execute("SELECT MAX(BarDate) FROM market.PriceBar WHERE Symbol=? AND Source='tiingo'", symbol)
    row = cur.fetchone()
    return row[0] if row and row[0] else None

# --------------------
# Rate limiter (token buckets)
# --------------------
class RateLimiter:
    def __init__(self, per_min, per_day):
        self.per_min = per_min
        self.per_day = per_day
        self.min_tokens = per_min
        self.day_tokens = per_day
        self.last_min = datetime.utcnow().replace(second=0, microsecond=0)
        self.last_day = datetime.utcnow().date()
        self.lock = threading.Lock()

    def _refill(self):
        now = datetime.utcnow()
        # minute bucket
        if now >= self.last_min + timedelta(minutes=1):
            self.min_tokens = self.per_min
            self.last_min = now.replace(second=0, microsecond=0)
        # day bucket
        if now.date() != self.last_day:
            self.day_tokens = self.per_day
            self.last_day = now.date()

    def take(self, tokens=1):
        with self.lock:
            while True:
                self._refill()
                if self.min_tokens >= tokens and self.day_tokens >= tokens:
                    self.min_tokens -= tokens
                    self.day_tokens -= tokens
                    return
                # Sleep until refill with small jitter
                time.sleep(0.5 + random.random()*0.5)

    def snapshot(self):
        with self.lock:
            return {
                "minute_remaining": self.min_tokens,
                "minute_limit": self.per_min,
                "day_remaining": self.day_tokens,
                "day_limit": self.per_day,
                "last_minute_reset": self.last_min.isoformat()+'Z',
                "last_day_reset": self.last_day.isoformat(),
            }

limiter = RateLimiter(MAX_CALLS_PER_MINUTE, MAX_CALLS_PER_DAY)

# --------------------
# Tiingo ingestion
# --------------------
client = TiingoClient()  # reads TIINGO_API_KEY

def parse_price(item):
    # tiingo EOD fields: date, open, high, low, close, volume, adjClose
    d = dtparse.isoparse(item["date"]).date()
    return {
        "BarDate": d.isoformat(),
        "Open": item.get("open"),
        "High": item.get("high"),
        "Low":  item.get("low"),
        "Close": item.get("close"),
        "Volume": item.get("volume"),
        "AdjClose": item.get("adjClose"),
    }

def fetch_and_store_eod():
    """Incremental EOD fetch for all symbols with quota & backoff."""
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            ensure_table()  # harmless if already created
            today_iso = date.today().isoformat()
            for sym in SYMBOLS:
                # Compute start date (resume)
                last = max_bar_date(cur, sym)
                start_iso = (last + timedelta(days=1)).isoformat() if last else INIT_START_DATE
                if dtparse.isoparse(start_iso).date() > date.today():
                    continue  # nothing to do
                # Call Tiingo
                success = False
                retries = 0
                while not success and retries < 5:
                    try:
                        limiter.take(1)
                        data = client.get_ticker_price(
                            sym, startDate=start_iso, endDate=today_iso, frequency="daily"
                        )
                        rows = []
                        for it in data:
                            row = parse_price(it)
                            row["symbol"] = sym
                            rows.append(row)
                        for r in rows:
                            upsert_row(cur, r)
                        success = True
                    except Exception as e:
                        # Backoff on transient issues (429/5xx/connection)
                        wait = min(60, 2 ** retries) + random.uniform(0, 0.5)
                        print(f"[ingest] {sym} error: {e}; retry in {wait:.1f}s", flush=True)
                        time.sleep(wait)
                        retries += 1
    except Exception as e:
        print("[ingest] fatal:", e, flush=True)

# --------------------
# API
# --------------------
app = FastAPI()

@app.on_event("startup")
def on_start():
    # Ensure schema quickly; don't crash if DB down—just log
    try:
        ensure_table()
    except Exception as e:
        print("[startup] ensure_table error:", e, flush=True)
    # Start scheduler
    if ENABLE_SCHEDULER:
        sched = BackgroundScheduler()
        sched.add_job(fetch_and_store_eod, "interval",
                      minutes=FETCH_INTERVAL_MINUTES,
                      next_run_time=datetime.now()+timedelta(seconds=20),  # small delay
                      max_instances=1, coalesce=True, misfire_grace_time=60)
        sched.start()
        app.state.sched = sched

@app.get("/healthz")
def healthz():
    # quick DB ping (non-fatal)
    ok = True
    try:
        with get_conn() as c: c.cursor().execute("SELECT 1")
    except Exception:
        ok = False
    return {"ok": ok, "symbols": SYMBOLS, "scheduler": ENABLE_SCHEDULER}

@app.get("/limits")
def limits():
    return limiter.snapshot()

@app.get("/prices/latest")
def latest(symbol: str):
    sym = symbol.upper()
    with get_conn() as c:
        cur = c.cursor()
        cur.execute("""
          SELECT TOP (1) BarDate,[Open],[High],[Low],[Close],Volume
          FROM market.PriceBar
          WHERE Symbol=? AND Source='tiingo'
          ORDER BY BarDate DESC
        """, sym)
        r = cur.fetchone()
    if not r:
        return {"symbol": sym, "data": None}
    d,o,h,l,cl,v = r
    return {"symbol": sym, "date": d.isoformat(), "open": o, "high": h, "low": l, "close": cl, "volume": v}

@app.get("/prices/range")
def range_prices(symbol: str, start: str, end: Optional[str]=None):
    sym = symbol.upper()
    end = end or date.today().isoformat()
    with get_conn() as c:
        cur = c.cursor()
        cur.execute("""
          SELECT BarDate,[Open],[High],[Low],[Close],Volume
          FROM market.PriceBar
          WHERE Symbol=? AND Source='tiingo' AND BarDate BETWEEN ? AND ?
          ORDER BY BarDate ASC
        """, sym, start, end)
        rows = cur.fetchall()
    data = [{"date": r[0].isoformat(), "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]} for r in rows]
    return {"symbol": sym, "count": len(data), "data": data}

@app.post("/ingest/run")
def ingest_now():
    threading.Thread(target=fetch_and_store_eod, daemon=True).start()
    return {"started": True}
