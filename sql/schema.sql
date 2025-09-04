PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS stations (
  station_id TEXT PRIMARY KEY,
  name TEXT,
  lat REAL,
  lon REAL,
  capacity INTEGER,
  address TEXT,
  rental_methods TEXT,
  is_virtual_station INTEGER,
  external_id TEXT,
  short_name TEXT,
  region_id TEXT,
  last_updated INTEGER
);

CREATE TABLE IF NOT EXISTS station_status (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  station_id TEXT NOT NULL,
  num_bikes_available INTEGER,
  num_bikes_disabled INTEGER,
  num_docks_available INTEGER,
  num_docks_disabled INTEGER,
  is_installed INTEGER,
  is_renting INTEGER,
  is_returning INTEGER,
  last_reported INTEGER,
  scraped_at TEXT NOT NULL,
  vehicles_json TEXT,
  FOREIGN KEY (station_id) REFERENCES stations (station_id)
);
CREATE INDEX IF NOT EXISTS idx_station_status_station_time ON station_status(station_id, scraped_at);

CREATE TABLE IF NOT EXISTS weather_hourly (
  time TEXT PRIMARY KEY,
  temperature_2m REAL,
  precipitation REAL,
  rain REAL,
  showers REAL,
  snowfall REAL,
  cloudcover REAL,
  windspeed_10m REAL,
  relative_humidity_2m REAL,
  weathercode INTEGER
);
