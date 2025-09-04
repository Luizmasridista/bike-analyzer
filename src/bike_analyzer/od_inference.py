from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .utils import haversine


@dataclass
class Node:
    station_id: str
    lat: float
    lon: float
    count: int


def _match_flows_once(departs: list[Node], arrives: list[Node]) -> list[tuple[str, str, int]]:
    flows: list[tuple[str, str, int]] = []
    # Greedy nearest-neighbor matching
    while True:
        dep_candidates = [d for d in departs if d.count > 0]
        arr_candidates = [a for a in arrives if a.count > 0]
        if not dep_candidates or not arr_candidates:
            break
        # pick departure with largest remaining to stabilize
        dep = max(dep_candidates, key=lambda x: x.count)
        # find nearest arrival
        best = None
        best_dist = 1e18
        for a in arr_candidates:
            dist = haversine(dep.lat, dep.lon, a.lat, a.lon)
            if dist < best_dist:
                best = a
                best_dist = dist
        if best is None:
            break
        flow = min(dep.count, best.count)
        flows.append((dep.station_id, best.station_id, flow))
        dep.count -= flow
        best.count -= flow
    return flows


def infer_flows(status_df: pd.DataFrame, stations_df: pd.DataFrame, freq: str = "10min") -> pd.DataFrame:
    # status_df: station_id, scraped_at (ISO), num_bikes_available
    df = status_df.copy()
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])  # local tz strings OK
    df["bucket"] = df["scraped_at"].dt.floor(freq)
    df = df.sort_values(["station_id", "bucket", "scraped_at"])  # keep last per bucket
    df_last = df.groupby(["station_id", "bucket"], as_index=False).last()
    df_last["delta"] = df_last.groupby("station_id")["num_bikes_available"].diff().fillna(0).astype(int)

    st = stations_df.set_index("station_id")[['lat','lon']]

    all_flows: list[tuple[pd.Timestamp, str, str, int]] = []
    for bucket, g in df_last.groupby("bucket"):
        departs: list[Node] = []
        arrives: list[Node] = []
        for _, row in g.iterrows():
            sid = row["station_id"]
            if sid not in st.index:
                continue
            lat, lon = st.loc[sid, "lat"], st.loc[sid, "lon"]
            d = int(row["delta"])
            if d < 0:
                departs.append(Node(sid, lat, lon, -d))
            elif d > 0:
                arrives.append(Node(sid, lat, lon, d))
        if departs and arrives:
            matched = _match_flows_once(departs, arrives)
            for o, d, c in matched:
                all_flows.append((bucket, o, d, c))

    flows_df = pd.DataFrame(all_flows, columns=["bucket", "o", "d", "count"]).groupby(["o", "d"], as_index=False)["count"].sum()
    return flows_df
