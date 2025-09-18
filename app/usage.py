from __future__ import annotations
from datetime import date, datetime
from zoneinfo import ZoneInfo
from sqlalchemy import text
from .config import settings
from .ingest import get_engine

SERVICE_NAME = "tiingo"

def _now_local() -> datetime:
    return datetime.now(ZoneInfo(settings.TIMEZONE))

def _today_local() -> str:
    return _now_local().date().isoformat()

def _today() -> str:
    return date.today().isoformat()

def _hour_key(when: datetime | None = None):
    dt = when or _now_local()
    return dt.date().isoformat(), dt.hour

def _ensure_daily_row():
    with get_engine().begin() as conn:
        conn.execute(text(
            f"""
            MERGE [{settings.SQLSERVER_DB_SCHEMA}].[ApiUsage] AS t
            USING (SELECT CAST(:d AS DATE) d, CAST(:s AS NVARCHAR(50)) s) AS src
            ON t.UsageDate = src.d AND t.Service = src.s
            WHEN NOT MATCHED THEN INSERT ([UsageDate],[Service],[Calls]) VALUES (src.d, src.s, 0);
            """
        ), {"d": _today(), "s": SERVICE_NAME})

def _ensure_hourly_row(d: str, h: int):
    with get_engine().begin() as conn:
        conn.execute(text(f"""
            MERGE [{settings.SQLSERVER_DB_SCHEMA}].[ApiUsageHourly] AS t
            USING (SELECT CAST(:d AS DATE) d, CAST(:h AS TINYINT) h, CAST(:s AS NVARCHAR(50)) s) AS src
            ON t.UsageDate = src.d AND t.UsageHour = src.h AND t.Service = src.s
            WHEN NOT MATCHED THEN INSERT ([UsageDate],[UsageHour],[Service],[Calls]) VALUES (src.d, src.h, src.s, 0);
        """), {"d": d, "h": h, "s": SERVICE_NAME})

def increment_calls(n: int = 1, when: datetime | None = None) -> None:
    d, h = _hour_key(when)
    _ensure_daily_row()
    _ensure_hourly_row(d, h)
    with get_engine().begin() as conn:
        conn.execute(text(f"""
            UPDATE [{settings.SQLSERVER_DB_SCHEMA}].[ApiUsage] SET [Calls] = [Calls] + :n
            WHERE [UsageDate] = :d AND [Service] = :s;
        """), {"n": n, "d": d, "s": SERVICE_NAME})
        conn.execute(text(f"""
            UPDATE [{settings.SQLSERVER_DB_SCHEMA}].[ApiUsageHourly] SET [Calls] = [Calls] + :n
            WHERE [UsageDate] = :d AND [UsageHour] = :h AND [Service] = :s;
        """), {"n": n, "d": d, "h": h, "s": SERVICE_NAME})

def calls_today() -> int:
    _ensure_daily_row()
    with get_engine().begin() as conn:
        val = conn.execute(text(
            f"""
            SELECT [Calls] FROM [{settings.SQLSERVER_DB_SCHEMA}].[ApiUsage]
            WHERE [UsageDate] = :d AND [Service] = :s
            """
        ), {"d": _today(), "s": SERVICE_NAME}).scalar()
    return int(val or 0)

def calls_this_hour() -> int:
    d, h = _hour_key(None)
    with get_engine().begin() as conn:
        val = conn.execute(text(f"""
            SELECT [Calls] FROM [{settings.SQLSERVER_DB_SCHEMA}].[ApiUsageHourly]
            WHERE [UsageDate] = :d AND [UsageHour] = :h AND [Service] = :s
        """), {"d": d, "h": h, "s": SERVICE_NAME}).scalar()
    return int(val or 0)

def hourly_breakdown(date_str: str | None = None) -> dict:
    d = date_str or _today()
    buckets = {h: 0 for h in range(24)}
    with get_engine().begin() as conn:
        rows = conn.execute(text(f"""
            SELECT UsageHour, Calls
            FROM [{settings.SQLSERVER_DB_SCHEMA}].[ApiUsageHourly]
            WHERE [UsageDate] = :d AND [Service] = :s
        """), {"d": d, "s": SERVICE_NAME}).all()
    for h, c in rows:
        buckets[int(h)] = int(c or 0)
    return buckets

def calls_left_today() -> int | None:
    if not settings.MAX_API_CALLS_PER_DAY:
        return None
    left = settings.MAX_API_CALLS_PER_DAY - settings.API_CALLS_BUFFER - calls_today()
    return max(0, left)

def can_make_call() -> bool:
    cl = calls_left_today()
    return True if cl is None else cl > 0


    