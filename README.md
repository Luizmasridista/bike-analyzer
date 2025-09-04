# Bike Analyzer (Porto Alegre)

Projeto de análise de mobilidade com dados do sistema de bicicletas compartilhadas de Porto Alegre (BikePoA) via GBFS e clima via Open‑Meteo. Pipeline em Python + SQLite, consultas SQL e notebook de EDA.

## Dados
- GBFS BikePoA (auto-discovery): https://portoalegre.publicbikesystem.net/customer/gbfs/v2/gbfs.json
- Clima: Open‑Meteo (https://open-meteo.com/)

## Estrutura
- `src/bike_analyzer/`: código de ETL e utilidades
- `sql/schema.sql`: esquema do banco (SQLite)
- `sql/queries.sql`: consultas de KPIs
- `data/`: base local (`data/bikepoa.sqlite`) e arquivos auxiliares
- `notebooks/01_eda.ipynb`: exploração inicial e insights

## Requisitos
```bash
python >= 3.10
pip install -r requirements.txt
```

## Uso rápido
Inicialize o banco, ingira metadados de estações, colete um snapshot de status e clima:
```bash
python -m bike_analyzer.cli init-db
python -m bike_analyzer.cli ingest-stations
python -m bike_analyzer.cli ingest-status
python -m bike_analyzer.cli ingest-weather --start -2d --end +2d
```
As consultas sugeridas estão em `sql/queries.sql`. Abra o notebook para EDA.

## Ideias de análises
- Utilização por estação (capacidade vs. bikes disponíveis)
- Padrões por hora/dia da semana e sazonalidade
- Correlação com clima (chuva, temperatura, vento)
- Disponibilidade crítica (estações frequentemente cheias/vazias)

## Licença
MIT