from __future__ import annotations

import math
from typing import Iterable

import pandas as pd
from sqlalchemy import text

from .db import get_engine


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def get_stations() -> pd.DataFrame:
    eng = get_engine()
    q = text(
        """
        SELECT station_id, name, lat, lon, capacity
        FROM stations
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        """
    )
    with eng.connect() as conn:
        df = pd.read_sql(q, conn)
    return df


def get_status_range(start: str | None = None, end: str | None = None) -> pd.DataFrame:
    eng = get_engine()
    sql = "SELECT station_id, scraped_at, num_bikes_available FROM station_status"
    params: dict[str, str] = {}
    where: list[str] = []
    if start:
        where.append("scraped_at >= :start")
        params["start"] = start
    if end:
        where.append("scraped_at <= :end")
        params["end"] = end
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY scraped_at"
    with eng.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    return df


def get_time_bounds() -> tuple[str | None, str | None]:
    eng = get_engine()
    with eng.connect() as conn:
        res = conn.execute(text("SELECT MIN(scraped_at), MAX(scraped_at) FROM station_status"))
        row = res.fetchone()
        if row:
            return row[0], row[1]
    return None, None
