from __future__ import annotations

import argparse
import json

from .db import init_db
from .etl_gbfs import ingest_once
from .etl_weather import fetch_weather, load_weather_hourly


def main() -> None:
    parser = argparse.ArgumentParser(prog="bike-analyzer")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db")
    sub.add_parser("ingest-stations")
    sub.add_parser("ingest-status")

    p_w = sub.add_parser("ingest-weather")
    p_w.add_argument("--start", default="-2d", help="Data inicial (YYYY-MM-DD) ou relativo, ex: -2d")
    p_w.add_argument("--end", default="+2d", help="Data final (YYYY-MM-DD) ou relativo, ex: +2d")

    args = parser.parse_args()

    if args.cmd == "init-db":
        init_db()
        print("ok")
        return

    if args.cmd in {"ingest-stations", "ingest-status"}:
        res = ingest_once()
        print(json.dumps(res))
        return

    if args.cmd == "ingest-weather":
        payload = fetch_weather(args.start, args.end)
        n = load_weather_hourly(payload)
        print(json.dumps({"rows": n}))
        return


if __name__ == "__main__":
    main()
