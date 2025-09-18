from __future__ import annotations
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import text
from typing import List, Optional
import logging
import math


from .config import settings
from .ingest import run_ingest_once, last_run_utc, get_engine
from .ingest_intraday import sync_intraday_for_all_symbols, sync_intraday_for_symbol, now
from .usage import calls_today, calls_left_today, calls_this_hour

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tiingo-layer")

app = FastAPI(title="Trading Data Layer", version="1.1.0")

origins = [
    "https://brave-meadow-0b7a0fa1e.1.azurestaticapps.net",
    "http://localhost:8888",
    "http://127.0.0.1:8888",
    # add prod origins too, e.g. "https://app.smartpowerai.org"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,          # or use allow_origin_regex=r"https://.*\.smartpowerai\.org$"
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],            # or list specific headers
)

scheduler = BackgroundScheduler(timezone=settings.TIMEZONE)

EOD_Scheduler_Id = "ingest-eod"
IntraDay_Scheduler_Id = "ingest-intraday"

def scheduler_eod_jobId():
    return EOD_Scheduler_Id

def scheduler_eod_jobId():
    return EOD_Scheduler_Id

def _schedule_eod_job():
    if settings.SQLSERVER_SCHEDULE_CRON:
        # format: "m h dom mon dow" e.g. "30 23 * * *"
        minute, hour, dom, mon, dow = settings.SQLSERVER_SCHEDULE_CRON.split()
        trigger = CronTrigger(minute=minute, hour=hour, day=dom, month=mon, day_of_week=dow)
        scheduler.add_job(run_ingest_once, trigger, id=EOD_Scheduler_Id, replace_existing=True)
        logger.info(f"Scheduled EOD via CRON: {settings.SQLSERVER_SCHEDULE_CRON}")
    else:
        minutes = settings.FETCH_INTERVAL_MINUTES or 1440
        trigger = IntervalTrigger(minutes=minutes)
        scheduler.add_job(run_ingest_once, trigger, id=EOD_Scheduler_Id, replace_existing=True)
        logger.info(f"Scheduled EOD every {minutes} minutes")

def _compute_intraday_interval_seconds(symbol_count: int) -> int:
    if settings.INTRADAY_INTERVAL_SECONDS:
        # If MAX_API_CALLS_PER_HOUR is defined, derive a cadence that stays under (max -buffer)
        if settings.MAX_API_CALLS_PER_HOUR:
            budget = max(1, settings.MAX_API_CALLS_PER_HOUR - int(settings.MAX_API_CALLS_PER_HOUR)/6)
            interval = int(math.ceil(3600 * symbol_count / max(1, budget)))
            hour_rate = max(30, interval)
            # return(max(30, interval))
        # If MAX_API_CALLS_PER_DAY is defined, derive a cadence that stays under (max - buffer)
        if settings.MAX_API_CALLS_PER_DAY:
            budget = max(1, settings.MAX_API_CALLS_PER_DAY - settings.API_CALLS_BUFFER)
            # Reserve symbol_count calls for nightly EOD (rough estimate)
            budget = max(1, budget - symbol_count - 2)   # insertamos un numero reservado de peticiones para mantener los tiempos de peticion levemente altos y asegurar la estabilidad
            # Each intraday cycle makes `symbol_count` API calls (1 per symbol)
            # cycles_per_day <= budget / symbol_count
            # interval_sec >= 86400 / cycles_per_day => 86400 * symbol_count / budget
            interval = int(math.ceil(86400 * symbol_count / max(1, budget)))
            day_rate = max(30, interval)
            # return max(30, interval) # never faster than 30s by default

        print("day_rate: ", day_rate)
        print("hour_rate: ", hour_rate)
        print("INTRADAY_INTERVAL_SECONDS: ", int(settings.INTRADAY_INTERVAL_SECONDS))

        return max(15, int(settings.INTRADAY_INTERVAL_SECONDS), day_rate, hour_rate)
    
    # Fallback: 60s cycle
    return 60

def _schedule_intraday_job():
    if not settings.INTRADAY_ENABLED:
        logger.info("Intraday sync disabled")
        return
    # symbols = [s.strip() for s in settings.SYMBOLS.split(',') if s.strip()]
    symbols = settings.SYMBOLS
    interval_sec = _compute_intraday_interval_seconds(len(symbols))
    print(now())
    print("Intraday interval [sec]: ", interval_sec)
    trigger = IntervalTrigger(seconds=interval_sec)
    scheduler.add_job(sync_intraday_for_all_symbols, trigger, id=IntraDay_Scheduler_Id, replace_existing=True)
    logger.info(f"Scheduled INTRADAY every {interval_sec}s for {len(symbols)} symbol(s)")

def getJobsList():
    jobs = scheduler.get_jobs()
    for job in jobs:
        print(f"Job ID: {job.id}, Function: {job.func.__name__}, Next run time: {job.next_run_time}")
    return jobs

@app.on_event("startup")
def _on_startup():
    # Ensure DB ready and schedule jobs
    get_engine() # warms engine and ensures schema/tables
    _schedule_eod_job()
    _schedule_intraday_job()
    scheduler.start()

    logger.info("Service started")

@app.on_event("shutdown")
def _on_shutdown():
    scheduler.shutdown(wait=False)

@app.get("/healthz")
def healthz():
    # Simple DB ping
    try:
        with get_engine().begin() as conn:
            conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False
    jobs = {j.id: (j.next_run_time.isoformat() if j.next_run_time else None) for j in scheduler.get_jobs()}
    return {
        "status": "ok",
        "db": db_ok,
        "last_eod_run_utc": last_run_utc(),
        "next_runs": jobs,
        "calls_today": calls_today(),
        "calls_left_today": calls_left_today(),
        "calls_left_hour": calls_this_hour(),
    }

@app.post("/daemon/start")
def start_daemon():
    job = scheduler.get_job(IntraDay_Scheduler_Id)
    if job:
        return {"message": "Daemon already running"}
    # scheduler.add_job(sync_intraday_for_symbol, "interval", minutes=2, id=IntraDay_Scheduler_Id)
    _schedule_intraday_job()
    return {"message": "Daemon started"}

@app.post("/daemon/stop")
def stop_daemon():
    job = scheduler.get_job(IntraDay_Scheduler_Id)
    if not job:
        return {"message": "Daemon not running"}
    scheduler.remove_job(IntraDay_Scheduler_Id)
    return {"message": "Daemon stopped"}

@app.get("/daemon/status")
def daemon_status():
    job = scheduler.get_job(IntraDay_Scheduler_Id)
    return {"running": job is not None}

# Seed EOD history (recommended first)
@app.post("/prices/sync")
def sync_now():
    return run_ingest_once()

# Seed intraday (optional, for “today” minutes)
@app.post("/prices/intraday/sync")
def intraday_sync(symbol: Optional[str] = Query(None), window_minutes: Optional[int] = Query(None)):
    print("into /prices/intraday/sync")
    if symbol:
        res = sync_intraday_for_symbol(symbol.upper(), window_minutes)
        return {"data": {symbol.upper(): res}}
    else:
        return {"data": sync_intraday_for_all_symbols(window_minutes)}
    
@app.get("/prices/latest")
def latest_prices(symbol: Optional[str] = Query(None, description="If omitted, returns latest for all configured symbols")):
    print("/prices/latest - arg symbol: ", symbol)
    symbols: List[str]
    if symbol:
        print("if symbol")
        symbols = [symbol.upper()]
    else:
        print("else symbol")
        symbols = [s.strip().upper() for s in settings.SYMBOLS if s.strip()]

    print("baked symbols: ", symbols)

    results = []
    sql = text(
        f"""
        SELECT TOP (1) [Symbol],[Source],[BarDate],[Open],[High],[Low],[Close],[Volume],[AdjClose]
        FROM [{settings.SQLSERVER_DB_SCHEMA}].[PriceBar]
        WHERE [Symbol] = :symbol AND [Source] = :source
        ORDER BY [BarDate] DESC
        """
    )
    with get_engine().begin() as conn:
        for sym in symbols:
            row = conn.execute(sql, {"symbol": sym, "source": settings.SOURCE_EOD}).mappings().first()
            if row:
                results.append(dict(row))
    return {"data": results}

@app.get("/usage")
def usage():
    print("Printing jobs: ",getJobsList())
    return {"calls_today": calls_today(), "calls_this_hour": calls_this_hour(),"calls_left_today": calls_left_today()}

@app.get("/prices/history")
def eod_history(
    symbol: str = Query(..., description="Ticker symbol, e.g., MSFT"),
    start: Optional[str] = Query(None, description="YYYY-MM-DD (inclusive)"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD (inclusive)"),
    order: str = Query("asc", pattern="^(?i)(asc|desc)$", description="Sort by date"),
    ):
    print("into /prices/history")
    # Return EOD bars from PriceBar for a date range (inclusive).
    symbol = symbol.upper()
    params = {"symbol": symbol, "source": settings.SOURCE_EOD}
    clauses = ["[Symbol] = :symbol", "[Source] = :source"]
    if start:
        params["start"] = start
        clauses.append("[BarDate] >= :start")
    if end:
        params["end"] = end
        clauses.append("[BarDate] <= :end")

    sql = text(
        f"""
        SELECT [Symbol],[Source],[BarDate],[Open],[High],[Low],[Close],[Volume],[AdjClose]
        FROM [{settings.SQLSERVER_DB_SCHEMA}].[PriceBar]
        WHERE [Symbol] = :symbol AND [Source] = :source
        AND (:start IS NULL OR [BarDate] >= CONVERT(date, :start))
        AND (:end   IS NULL OR [BarDate] <= CONVERT(date, :end))
        ORDER BY [BarDate] {"ASC" if order.lower()=="asc" else "DESC"}
    """
    )
    print("execution query: ", sql, params)
    with get_engine().begin() as conn:
        rows = conn.execute(sql, params).mappings().all()

    print("returned rows: ", rows)
    return {"data": [dict(r) for r in rows]}

@app.get("/prices/intraday/history")
def intraday_history(
    symbol: str = Query(..., description="Ticker symbol, e.g., MSFT"),
    start: Optional[str] = Query(None, description="ISO date or datetime; filters BarTime >= start"),
    end: Optional[str] = Query(None, description="ISO date or datetime; filters BarTime <= end"),
    interval_sec: Optional[int] = Query(None, description="Override interval in seconds (default from config)"),
    order: str = Query("asc", pattern="^(?i)(asc|desc)$"),
    limit: Optional[int] = Query(None, ge=1, le=100000),
    ):
    """Return intraday bars from PriceBarIntra for a time range (inclusive)."""
    symbol = symbol.upper()
    isec = interval_sec or _interval_seconds_from_config()


    params = {"symbol": symbol, "source": "tiingo_iex", "isec": isec}
    clauses = ["[Symbol] = :symbol", "[Source] = :source", "[IntervalSec] = :isec"]
    if start:
        params["start"] = start
        clauses.append("[BarTime] >= :start")
    if end:
        params["end"] = end
        clauses.append("[BarTime] <= :end")
    
    order_sql = "ASC" if order.lower() == "asc" else "DESC"
    top_sql = f"TOP ({int(limit)}) " if limit else ""


    sql = text(
        f"""
        SELECT {top_sql}[Symbol],[Source],[BarTime],[IntervalSec],[Open],[High],[Low],[Close],[Volume]
        FROM [{settings.SQLSERVER_DB_SCHEMA}].[PriceBarIntra]
        WHERE {" AND ".join(clauses)}
        ORDER BY [BarTime] {order_sql}
        """
    )
    with get_engine().begin() as conn:
        rows = conn.execute(sql, params).mappings().all()
    return {"data": [dict(r) for r in rows]}

def _interval_seconds_from_config() -> int:
    r = (settings.INTRADAY_RESAMPLE or "1min").strip().lower()
    if r.endswith("min"):
        n = r[:-3]
        return int(n) * 60 if n.isdigit() else 60
    if r.endswith("sec"):
        n = r[:-3]
        return int(n) if n.isdigit() else 60
    return 60

def _fetch_latest_intraday(symbol: str):
    isec = _interval_seconds_from_config()
    sql = text(
        f"""
        SELECT TOP (1)
        [Symbol],[Source],[BarTime],[IntervalSec],[Open],[High],[Low],[Close],[Volume]
        FROM [{settings.SQLSERVER_DB_SCHEMA}].[PriceBarIntra]
        WHERE [Symbol] = :symbol AND [Source] = 'tiingo_iex' AND [IntervalSec] = :isec
        ORDER BY [BarTime] DESC
        """
    )
    with get_engine().begin() as conn:
        row = conn.execute(sql, {"symbol": symbol, "isec": isec}).mappings().first()
        return (dict(row) if row else None)