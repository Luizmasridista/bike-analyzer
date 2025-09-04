import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent / 'src'))

import time
from typing import Optional

import numpy as np
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

from bike_analyzer.config import CITY_LAT, CITY_LON
from bike_analyzer.utils import get_stations, get_status_range, get_time_bounds
from bike_analyzer.od_inference import infer_flows
from bike_analyzer.db import init_db, get_engine
from bike_analyzer.etl_gbfs import ingest_once
from bike_analyzer.etl_weather import fetch_weather, load_weather_hourly

st.set_page_config(page_title="Bike Analyzer – Porto Alegre", layout="wide")

@st.cache_data(show_spinner=False)
def load_stations_cached():
    return get_stations()

@st.cache_data(show_spinner=False)
def load_status_cached(start: Optional[str], end: Optional[str]):
    return get_status_range(start, end)

@st.cache_data(show_spinner=False)
def get_bounds():
    return get_time_bounds()


def check_data_exists() -> dict[str, int]:
    """Verifica se há dados no banco."""
    engine = get_engine()
    with engine.connect() as conn:
        from sqlalchemy import text
        try:
            stations_count = conn.execute(text("SELECT COUNT(*) FROM stations")).scalar()
            status_count = conn.execute(text("SELECT COUNT(*) FROM station_status")).scalar()
            return {"stations": stations_count or 0, "status": status_count or 0}
        except:
            return {"stations": 0, "status": 0}


def run_initial_ingest():
    """Roda a ingestão inicial de dados."""
    try:
        # Inicializar banco se necessário
        init_db()
        
        # Ingerir estações e status
        with st.spinner("Coletando dados das estações BikePoA..."):
            result = ingest_once()
            st.success(f"✅ {result['stations_upserted']} estações, {result['status_rows']} snapshots coletados")
        
        # Ingerir clima dos últimos 2 dias
        try:
            with st.spinner("Coletando dados climáticos..."):
                weather_data = fetch_weather("-2d", "+1d")
                weather_rows = load_weather_hourly(weather_data)
                st.success(f"✅ {weather_rows} registros de clima adicionados")
        except Exception as e:
            st.warning(f"⚠️ Clima falhou (opcional): {e}")
        
        # Limpar cache para recarregar dados
        st.cache_data.clear()
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ Erro na ingestão: {e}")
        return False
    return True


def geocode_bairros(stations: pd.DataFrame) -> pd.DataFrame:
    cache_path = Path("data/station_neighborhoods.csv")
    if cache_path.exists():
        return pd.read_csv(cache_path)
    out = []
    headers = {"User-Agent": "bike-analyzer/0.1 (educational)"}
    for _, row in stations.iterrows():
        lat, lon = row["lat"], row["lon"]
        try:
            r = requests.get(
                "https://nominatim.openstreetmap.org/reverse",
                params={
                    "format": "jsonv2",
                    "lat": lat,
                    "lon": lon,
                    "accept-language": "pt-BR",
                    "zoom": 14,
                },
                headers=headers,
                timeout=20,
            )
            r.raise_for_status()
            js = r.json()
            addr = js.get("address", {})
            bairro = (
                addr.get("neighbourhood")
                or addr.get("suburb")
                or addr.get("city_district")
                or addr.get("quarter")
                or addr.get("residential")
                or None
            )
        except Exception:
            bairro = None
        out.append({"station_id": row["station_id"], "bairro": bairro, "lat": lat, "lon": lon})
        time.sleep(1)  # respeitar limites do Nominatim
    df = pd.DataFrame(out)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache_path, index=False)
    return df


def header():
    st.title("🚲 Bike Analyzer – Porto Alegre")
    st.caption("Dashboard de análise de mobilidade urbana com dados do BikePoA (GBFS) + clima")
    
    data_counts = check_data_exists()
    if data_counts["stations"] == 0:
        st.warning("👋 **Bem-vindo!** Este é seu primeiro acesso. Clique em '🚀 Carregar dados iniciais' na barra lateral para começar a análise.")
    else:
        st.info(f"📊 Analisando **{data_counts['stations']} estações** com **{data_counts['status']} snapshots** coletados.")


def sidebar():
    st.sidebar.header("Dados")
    
    # Verificar se há dados
    data_counts = check_data_exists()
    
    if data_counts["stations"] == 0:
        st.sidebar.error("🚫 Sem dados ainda")
        if st.sidebar.button("🚀 Carregar dados iniciais", type="primary"):
            run_initial_ingest()
            return None
        st.sidebar.markdown("---")
        st.sidebar.info("Clique em 'Carregar dados iniciais' para começar!")
        return None
    else:
        st.sidebar.success(f"✅ {data_counts['stations']} estações, {data_counts['status']} snapshots")
        
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("🔄 Atualizar", help="Coletar novo snapshot"):
                run_initial_ingest()
        with col2:
            if st.button("🗑️ Limpar cache"):
                st.cache_data.clear()
                st.rerun()
    
    st.sidebar.header("Filtros")
    tmin, tmax = get_bounds()
    if not tmin:
        st.sidebar.warning("Sem dados de status ainda.")
        return None
    
    start = st.sidebar.text_input("Início (YYYY-MM-DD HH:MM:SS)", value=str(tmin))
    end = st.sidebar.text_input("Fim (YYYY-MM-DD HH:MM:SS)", value=str(tmax))
    bucket = st.sidebar.select_slider("Janela para OD (min)", options=[5,10,15,20,30,60], value=10)
    topn = st.sidebar.slider("Top fluxos (OD)", min_value=10, max_value=200, value=50, step=10)
    return {"start": start, "end": end, "bucket": bucket, "topn": topn}


def map_view_state():
    return pdk.ViewState(latitude=CITY_LAT, longitude=CITY_LON, zoom=12, pitch=0)


def tab_bairros(stations: pd.DataFrame, status: pd.DataFrame):
    st.subheader("Bairros que mais usam bikes (proxy)")
    st.caption("Proxy: soma das variações absolutas de bikes por estação no período, agregada por bairro via geocodificação OSM.")

    df = status.copy()
    if df.empty:
        st.info("Sem dados no intervalo selecionado.")
        return
    df["scraped_at"] = pd.to_datetime(df["scraped_at"]) 
    df = df.sort_values(["station_id", "scraped_at"])
    df["delta"] = df.groupby("station_id")["num_bikes_available"].diff().fillna(0).astype(int)
    usage = df.groupby("station_id", as_index=False)["delta"].apply(lambda s: int(np.abs(s).sum()))
    usage = usage.rename(columns={"delta": "activity"})

    stns = stations.merge(usage, on="station_id", how="left").fillna({"activity":0})

    if st.button("Resolver bairros (OSM)"):
        bairro_df = geocode_bairros(stations)
    else:
        # tenta carregar se já existir
        p = Path("data/station_neighborhoods.csv")
        bairro_df = pd.read_csv(p) if p.exists() else pd.DataFrame(columns=["station_id","bairro","lat","lon"])

    if not bairro_df.empty:
        merged = stns.merge(bairro_df[["station_id","bairro"]], on="station_id", how="left")
        by_bairro = merged.groupby("bairro", as_index=False)["activity"].sum().sort_values("activity", ascending=False).head(20)
        st.dataframe(by_bairro, use_container_width=True)
        # Mapa: pontos ponderados por activity
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=merged,
            get_position="[lon, lat]",
            get_radius="np.clip(activity*5, 100, 3000)",
            get_fill_color="[255, 140, 0, 140]",
            pickable=True,
        )
        st.pydeck_chart(pdk.Deck(map_style="mapbox://styles/mapbox/light-v9", initial_view_state=map_view_state(), layers=[layer]))
    else:
        st.info("Clique em 'Resolver bairros (OSM)' para agregar por bairro. Enquanto isso, veja o heatmap por área.")
        # Heatmap por área (Hex)
        hex_layer = pdk.Layer(
            "HexagonLayer",
            data=stns,
            get_position="[lon, lat]",
            get_elevation_weight="activity",
            elevation_scale=1,
            elevation_range=[0, 5000],
            extruded=True,
            radius=200,
            coverage=1,
        )
        st.pydeck_chart(pdk.Deck(initial_view_state=map_view_state(), layers=[hex_layer]))


def tab_trajetos(stations: pd.DataFrame, status: pd.DataFrame, bucket_min: int, topn: int):
    st.subheader("Trajetos mais realizados (estimados)")
    st.caption("Estimativa baseada em variações de estoque por janela de tempo (matching de partidas/chegadas por proximidade). Não são viagens observadas.")
    if status.empty:
        st.info("Sem dados no intervalo selecionado.")
        return
    flows = infer_flows(status, stations, freq=f"{bucket_min}min")
    if flows.empty:
        st.info("Sem fluxos estimados no intervalo.")
        return
    # Coordenadas
    coords = stations.set_index("station_id")[['lat','lon']]
    flows = flows.sort_values("count", ascending=False).head(topn)
    flows = flows.assign(
        o_lat=lambda d: d["o"].map(coords["lat"]),
        o_lon=lambda d: d["o"].map(coords["lon"]),
        d_lat=lambda d: d["d"].map(coords["lat"]),
        d_lon=lambda d: d["d"].map(coords["lon"]),
    )
    st.dataframe(flows, use_container_width=True)

    arc = pdk.Layer(
        "ArcLayer",
        data=flows,
        get_source_position="[o_lon, o_lat]",
        get_target_position="[d_lon, d_lat]",
        get_width="np.clip(count, 1, 20)",
        get_source_color="[0, 128, 255, 160]",
        get_target_color="[255, 0, 128, 160]",
        pickable=True,
    )
    st.pydeck_chart(pdk.Deck(initial_view_state=map_view_state(), layers=[arc]))


def tab_bikes(stations: pd.DataFrame, status: pd.DataFrame):
    st.subheader("Onde geralmente tem mais bikes")
    st.caption("Média de bikes disponíveis por estação no período selecionado, com heatmap hexagonal.")
    if status.empty:
        st.info("Sem dados no intervalo selecionado.")
        return
    df = status.copy()
    df["scraped_at"] = pd.to_datetime(df["scraped_at"]) 
    df = df.sort_values(["station_id", "scraped_at"]) 
    avg_bikes = df.groupby("station_id")["num_bikes_available"].mean().reset_index(name="avg_bikes")
    stns = stations.merge(avg_bikes, on="station_id", how="left").fillna({"avg_bikes":0})

    layer_hex = pdk.Layer(
        "HexagonLayer",
        data=stns,
        get_position="[lon, lat]",
        get_elevation_weight="avg_bikes",
        elevation_scale=10,
        elevation_range=[0, 500],
        extruded=True,
        radius=200,
        coverage=1,
    )
    layer_pts = pdk.Layer(
        "ScatterplotLayer",
        data=stns,
        get_position="[lon, lat]",
        get_radius="np.clip(avg_bikes*20, 50, 2500)",
        get_fill_color="[0, 200, 100, 160]",
        pickable=True,
    )
    st.pydeck_chart(pdk.Deck(initial_view_state=map_view_state(), layers=[layer_hex, layer_pts]))


# App
header()
filters = sidebar()

# Só mostrar dashboard se há dados
if filters:
    stations = load_stations_cached()
    status = load_status_cached(filters["start"], filters["end"])

    tabs = st.tabs(["🏘️ Bairros", "🔄 Trajetos", "🚲 Bikes"])
    with tabs[0]:
        tab_bairros(stations, status)
    with tabs[1]:
        tab_trajetos(stations, status, filters["bucket"], filters["topn"])
    with tabs[2]:
        tab_bikes(stations, status)
else:
    # Placeholder quando não há dados
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.info("🏘️ **Bairros**\nHeatmap de uso por bairro (geocodificação OSM)")
    with col2:
        st.info("🔄 **Trajetos**\nFluxos OD estimados via matching temporal")
    with col3:
        st.info("🚲 **Bikes**\nHotspots de disponibilidade média")
