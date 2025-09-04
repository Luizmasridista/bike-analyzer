from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import requests
from sqlalchemy import text

from .config import GBFS_AUTO_DISCOVERY_URL
from .db import get_engine


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def fetch_auto_discovery() -> dict[str, Any]:
    r = requests.get(GBFS_AUTO_DISCOVERY_URL, timeout=30)
    r.raise_for_status()
    return r.json()


def _pick_feed_url(feeds: list[dict[str, Any]], feed_name: str) -> str | None:
    # Prefer empty language (common), then en, then pt/pt-BR, then first match
    lang_order = ["", "en", "pt", "pt-BR"]
    by_lang = {}
    for f in feeds:
        if f.get("name") == feed_name:
            by_lang.setdefault(f.get("language", ""), []).append(f["url"])
    for lang in lang_order:
        if lang in by_lang:
            return by_lang[lang][0]
    # fallback to any
    for f in feeds:
        if f.get("name") == feed_name:
            return f["url"]
    return None


def fetch_stations_and_status() -> tuple[dict[str, Any], dict[str, Any]]:
    auto = fetch_auto_discovery()
    # Try different structure patterns for feeds
    feeds = auto.get("data", {}).get("feeds", [])
    if not feeds:
        # Try nested structure like data.en.feeds
        for lang_key in ["en", "pt", "pt-BR"]:
            lang_data = auto.get("data", {}).get(lang_key, {})
            if "feeds" in lang_data:
                feeds = lang_data["feeds"]
                break
    
    if not feeds:
        raise RuntimeError("Nenhum feed encontrado na resposta GBFS")
    
    station_info_url = _pick_feed_url(feeds, "station_information")
    station_status_url = _pick_feed_url(feeds, "station_status")
    if not station_info_url or not station_status_url:
        raise RuntimeError("Feeds station_information/station_status não encontrados no GBFS")
    si = requests.get(station_info_url, timeout=30).json()
    ss = requests.get(station_status_url, timeout=30).json()
    return si, ss


def load_stations(si: dict[str, Any]) -> int:
    engine = get_engine()
    stations = si.get("data", {}).get("stations", [])
    rows = 0
    with engine.begin() as conn:
        for st in stations:
            conn.execute(
                text(
                    """
                    INSERT INTO stations (
                      station_id, name, lat, lon, capacity, address, rental_methods,
                      is_virtual_station, external_id, short_name, region_id, last_updated
                    ) VALUES (
                      :station_id, :name, :lat, :lon, :capacity, :address, :rental_methods,
                      :is_virtual_station, :external_id, :short_name, :region_id, :last_updated
                    )
                    ON CONFLICT(station_id) DO UPDATE SET
                      name=excluded.name,
                      lat=excluded.lat,
                      lon=excluded.lon,
                      capacity=excluded.capacity,
                      address=excluded.address,
                      rental_methods=excluded.rental_methods,
                      is_virtual_station=excluded.is_virtual_station,
                      external_id=excluded.external_id,
                      short_name=excluded.short_name,
                      region_id=excluded.region_id,
                      last_updated=excluded.last_updated
                    ;
                    """
                ),
                {
                    "station_id": st.get("station_id"),
                    "name": st.get("name"),
                    "lat": st.get("lat"),
                    "lon": st.get("lon"),
                    "capacity": st.get("capacity"),
                    "address": st.get("address"),
                    "rental_methods": ",".join(st.get("rental_methods", []) or []),
                    "is_virtual_station": int(bool(st.get("is_virtual_station"))),
                    "external_id": st.get("external_id"),
                    "short_name": st.get("short_name"),
                    "region_id": st.get("region_id"),
                    "last_updated": si.get("last_updated"),
                },
            )
            rows += 1
    return rows


def append_status_snapshot(ss: dict[str, Any]) -> int:
    engine = get_engine()
    stations = ss.get("data", {}).get("stations", [])
    scraped_at = _now_iso()
    rows = 0
    with engine.begin() as conn:
        for st in stations:
            vehicles_json = None
            if "vehicle_types_available" in st:
                vehicles_json = json.dumps(st.get("vehicle_types_available"))
            conn.execute(
                text(
                    """
                    INSERT INTO station_status (
                      station_id, num_bikes_available, num_bikes_disabled,
                      num_docks_available, num_docks_disabled, is_installed, is_renting,
                      is_returning, last_reported, scraped_at, vehicles_json
                    ) VALUES (
                      :station_id, :nba, :nbd, :nda, :ndd, :installed, :renting,
                      :returning, :last_reported, :scraped_at, :vehicles_json
                    );
                    """
                ),
                {
                    "station_id": st.get("station_id"),
                    "nba": st.get("num_bikes_available"),
                    "nbd": st.get("num_bikes_disabled"),
                    "nda": st.get("num_docks_available"),
                    "ndd": st.get("num_docks_disabled"),
                    "installed": st.get("is_installed"),
                    "renting": st.get("is_renting"),
                    "returning": st.get("is_returning"),
                    "last_reported": st.get("last_reported"),
                    "scraped_at": scraped_at,
                    "vehicles_json": vehicles_json,
                },
            )
            rows += 1
    return rows


def ingest_once() -> dict[str, Any]:
    si, ss = fetch_stations_and_status()
    n_stations = load_stations(si)
    n_status = append_status_snapshot(ss)
    return {"stations_upserted": n_stations, "status_rows": n_status}
