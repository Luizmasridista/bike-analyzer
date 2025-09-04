from __future__ import annotations

CITY_NAME = "Porto Alegre"
GBFS_AUTO_DISCOVERY_URL = "https://portoalegre.publicbikesystem.net/customer/gbfs/v2/gbfs.json"
DATABASE_URL = "sqlite:///data/bikepoa.sqlite"
TIMEZONE = "America/Sao_Paulo"
CITY_LAT = -30.0346
CITY_LON = -51.2177

WEATHER_HOURLY_PARAMS = {
    "hourly": [
        "temperature_2m",
        "precipitation",
        "rain",
        "showers",
        "snowfall",
        "cloudcover",
        "windspeed_10m",
        "relative_humidity_2m",
        "weathercode",
    ]
}