# Spark with Delta Lake on Docker

This project provides a Dockerized setup for running Apache Spark with Delta Lake support.  
It includes a custom Spark image with Delta dependencies and a simple example application.

## Project Layout

```
spark-deltalake-oss/
├─ app/                  # PySpark applications
│   └─ main.py
├─ data/                 # Input data (optional)
├─ warehouse/            # Delta tables stored here
├─ docker/
│   ├─ Dockerfile        # Custom Spark image
│   └─ requirements.txt  # Python dependencies
├─ config/               # Configuration files (optional)
└─ docker-compose.yml    # Compose services
```

## Prerequisites

- Docker
- Docker Compose v2

## Build and Run

From the project root:

```bash
docker compose up -d --build
```

Open a shell inside the container:

```bash
docker compose exec spark bash
```

Run the sample application:

```bash
spark-submit /app/main.py
```

## Spark UI

The Spark Web UI is available while jobs are running at:

http://localhost:4040

## Notes

- Delta tables are written under the `warehouse/` directory.
- Rebuild the image after changing `docker/Dockerfile` or `docker/requirements.txt`:

```bash
docker compose build --no-cache
```

- To shut down the environment:

```bash
docker compose down
```
