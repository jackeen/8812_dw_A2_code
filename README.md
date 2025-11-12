# Data warehouse Assignment 2

> clean game data and dig trends


## Run 

```sh
uv run main.py
```

## Dataset

> https://www.kaggle.com/datasets/fronkongames/steam-games-dataset?select=games.json

## Environment

### Docker

```sh
docker run -d --name clickhouse_dw -p 8123:8123 -p 9000:9000 -v /path/to/your/data:/var/lib/clickhouse
```

```sh
docker run -d --name postgres_dw -e POSTGRES_USER=user -e POSTGRES_PASSWORD=12345678 -e POSTGRES_DB=default -v postgres_dw:/var/lib/postgresql/data -p 5432:5432 postgres:latest
```

```sh
docker run -d -p 3000:3000 --name grafana_dw grafana/grafana:latest
```
admin 12345678
