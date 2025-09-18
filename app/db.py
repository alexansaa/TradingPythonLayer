from __future__ import annotations
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus

from .config import settings

# market.* is fixed by design; identifiers cannot be parameterized safely
DDL_ENSURE = """
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'market')
    EXEC(N'CREATE SCHEMA market');

IF OBJECT_ID(N'market.PriceBar', 'U') IS NULL
BEGIN
    CREATE TABLE market.PriceBar (
        Symbol   NVARCHAR(20)  NOT NULL,
        Source   NVARCHAR(32)  NOT NULL,
        BarDate  DATETIME2(0)  NOT NULL,
        [Open]   DECIMAL(18,6) NOT NULL,
        [High]   DECIMAL(18,6) NOT NULL,
        [Low]    DECIMAL(18,6) NOT NULL,
        [Close]  DECIMAL(18,6) NOT NULL,
        Volume   BIGINT        NULL,
        AdjClose DECIMAL(18,6) NULL,
        CONSTRAINT PK_market_PriceBar PRIMARY KEY CLUSTERED (Symbol, Source, BarDate)
    );

    CREATE INDEX IX_market_PriceBar_Symbol_BarDate
        ON market.PriceBar (Symbol, BarDate DESC)
        INCLUDE ([Close], Volume, AdjClose);
END
"""

def make_engine() -> Engine:
    # TrustServerCertificate avoids cert hassles in local/dev networks; tune for prod.
    conn_str = (
    f"mssql+pyodbc://{quote_plus(settings.SQLSERVER_USER)}:{quote_plus(settings.SQLSERVER_PASSWORD)}"
    f"@{settings.SQLSERVER_HOST}:{settings.SQLSERVER_PORT}/{settings.SQLSERVER_DB}?"
    f"driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"
    )
    print("conn_str: ", conn_str)
    engine = create_engine(conn_str, pool_pre_ping=True, pool_recycle=1800, future=True)
    return engine

def ensure_schema_and_table(engine: Engine) -> None:
    # Important: use exec_driver_sql so SQLAlchemy doesn't try to param-bind identifiers.
    with engine.begin() as conn:
        conn.exec_driver_sql(DDL_ENSURE)

def get_latest_date(engine: Engine, symbol: str, source: str) -> Optional[str]:
    with engine.begin() as conn:
        row = conn.execute(text(
            f"""
            SELECT CONVERT(varchar(10), MAX([BarDate]), 23)
            FROM [{settings.SQLSERVER_DB_SCHEMA}].[PriceBar]
            WHERE [Symbol] = :symbol AND [Source] = :source
            """
        ), {"symbol": symbol, "source": source}).scalar()
        return row # ISO string or None
    
MERGE_SQL = text(
    f"""
    MERGE [{settings.SQLSERVER_DB_SCHEMA}].[PriceBar] AS target
    USING (
        SELECT :Symbol AS Symbol, :Source AS Source, :BarDate AS BarDate
    ) AS src
    ON target.Symbol = src.Symbol AND target.Source = src.Source AND target.BarDate = src.BarDate
    WHEN MATCHED THEN UPDATE SET
        [Open] = :Open, [High] = :High, [Low] = :Low,
        [Close] = :Close, [Volume] = :Volume, [AdjClose] = :AdjClose
    WHEN NOT MATCHED THEN INSERT
        ([Symbol],[Source],[BarDate],[Open],[High],[Low],[Close],[Volume],[AdjClose])
        VALUES (:Symbol,:Source,:BarDate,:Open,:High,:Low,:Close,:Volume,:AdjClose);
    """
)

def upsert_bar(engine: Engine, payload: dict) -> None:
    with engine.begin() as conn:
        conn.execute(MERGE_SQL, payload)

# --- Intraday helpers ---
MERGE_INTRADAY = text(
    f"""
    MERGE [{settings.SQLSERVER_DB_SCHEMA}].[PriceBarIntra] AS t
    USING (SELECT :Symbol s, :Source src, :BarTime bt, :IntervalSec isec) AS src
    ON t.Symbol=src.s AND t.Source=src.src AND t.BarTime=src.bt AND t.IntervalSec=src.isec
    WHEN MATCHED THEN UPDATE SET [Open]=:Open,[High]=:High,[Low]=:Low,[Close]=:Close,[Volume]=:Volume
    WHEN NOT MATCHED THEN INSERT ([Symbol],[Source],[BarTime],[IntervalSec],[Open],[High],[Low],[Close],[Volume])
    VALUES (:Symbol,:Source,:BarTime,:IntervalSec,:Open,:High,:Low,:Close,:Volume);
    """
)

def upsert_intraday(engine: Engine, payload: dict) -> None:
    with engine.begin() as conn:
        conn.execute(MERGE_INTRADAY, payload)

def get_last_intraday_time(engine: Engine, symbol: str, source: str, interval_sec: int) -> Optional[str]:
    with engine.begin() as conn:
        row = conn.execute(text(
            f"""
            SELECT CONVERT(varchar(19), MAX([BarTime]), 120) -- yyyy-mm-dd hh:MM:ss
            FROM [{settings.SQLSERVER_DB_SCHEMA}].[PriceBarIntra]
            WHERE [Symbol] = :symbol AND [Source] = :source AND [IntervalSec] = :isec
            """
        ), {"symbol": symbol, "source": source, "isec": interval_sec}).scalar()
        return row # ISO-like string or None