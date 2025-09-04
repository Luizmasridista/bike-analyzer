from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from dateutil import parser as dateparser
from sqlalchemy import text

from .config import CITY_LAT, CITY_LON, TIMEZONE, WEATHER_HOURLY_PARAMS
from .db import get_engine


def _parse_rel(s: str) -> datetime:
    s = s.strip()
    now = datetime.now(timezone.utc)
    if s.endswith("d") and (s.startswith("+") or s.startswith("-")):
        days = int(s[:-1])
        return now + timedelta(days=days)
    return dateparser.parse(s)


def fetch_weather(start: str, end: str) -> dict[str, Any]:
    start_dt = _parse_rel(start)
    end_dt = _parse_rel(end)
    params = {
        "latitude": CITY_LAT,
        "longitude": CITY_LON,
        "timezone": TIMEZONE,
        "start_date": start_dt.date().isoformat(),
        "end_date": end_dt.date().isoformat(),
        "hourly": ",".join(WEATHER_HOURLY_PARAMS["hourly"]),
    }
    r = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def load_weather_hourly(payload: dict[str, Any]) -> int:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    engine = get_engine()
    cols = WEATHER_HOURLY_PARAMS["hourly"]
    rows = 0
    with engine.begin() as conn:
        for i, t in enumerate(times):
            data = {"time": t}
            for c in cols:
                data[c] = (hourly.get(c) or [None] * len(times))[i]
            conn.execute(
                text(
                    """
                    INSERT INTO weather_hourly (
                      time, temperature_2m, precipitation, rain, showers, snowfall,
                      cloudcover, windspeed_10m, relative_humidity_2m, weathercode
                    ) VALUES (
                      :time, :temperature_2m, :precipitation, :rain, :showers, :snowfall,
                      :cloudcover, :windspeed_10m, :relative_humidity_2m, :weathercode
                    )
                    ON CONFLICT(time) DO UPDATE SET
                      temperature_2m=excluded.temperature_2m,
                      precipitation=excluded.precipitation,
                      rain=excluded.rain,
                      showers=excluded.showers,
                      snowfall=excluded.snowfall,
                      cloudcover=excluded.cloudcover,
                      windspeed_10m=excluded.windspeed_10m,
                      relative_humidity_2m=excluded.relative_humidity_2m,
                      weathercode=excluded.weathercode
                    ;
                    """
                ),
                data,
            )
            rows += 1
    return rows
