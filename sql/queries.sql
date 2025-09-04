-- KPIs e consultas exemplo

-- 1) Resumo da rede na última coleta
WITH last_scrape AS (
  SELECT MAX(scraped_at) AS ts FROM station_status
)
SELECT
  COUNT(DISTINCT s.station_id) AS estaciones,
  SUM(COALESCE(s.capacity,0)) AS capacidade_total,
  SUM(ss.num_bikes_available) AS bikes_disp,
  SUM(ss.num_docks_available) AS docks_disp
FROM station_status ss
JOIN last_scrape ls ON ss.scraped_at = ls.ts
JOIN stations s ON s.station_id = ss.station_id;

-- 2) Top 10 estações por ocupação (bikes/capacidade) no último snapshot
WITH last_scrape AS (
  SELECT MAX(scraped_at) AS ts FROM station_status
)
SELECT
  s.station_id,
  s.name,
  s.capacity,
  ss.num_bikes_available,
  ROUND(100.0 * ss.num_bikes_available / NULLIF(s.capacity,0), 1) AS ocupacao_pct
FROM station_status ss
JOIN last_scrape ls ON ss.scraped_at = ls.ts
JOIN stations s ON s.station_id = ss.station_id
WHERE s.capacity IS NOT NULL AND s.capacity > 0
ORDER BY ocupacao_pct DESC
LIMIT 10;

-- 3) Série horária média de bikes disponíveis por hora do dia
SELECT
  s.station_id,
  s.name,
  STRFTIME('%H', ss.scraped_at) AS hora,
  AVG(ss.num_bikes_available) AS media_bikes
FROM station_status ss
JOIN stations s USING(station_id)
GROUP BY 1,2,3
ORDER BY s.name, hora;

-- 4) Correlação simples com clima (temperatura)
-- Agregar status por hora e juntar com clima
WITH status_hour AS (
  SELECT
    SUBSTR(scraped_at, 1, 13) || ':00:00' AS hora,
    AVG(num_bikes_available) AS bikes_med
  FROM station_status
  GROUP BY 1
)
SELECT
  wh.time AS hora,
  wh.temperature_2m,
  sh.bikes_med
FROM weather_hourly wh
JOIN status_hour sh ON sh.hora = wh.time
ORDER BY hora;