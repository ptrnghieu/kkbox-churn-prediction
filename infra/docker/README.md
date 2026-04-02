# Infrastructure — Docker Services

Local infrastructure for the disease trend detection pipeline.
All services run via Docker Compose.

---

## Services Overview

| Service | Directory | Ports | Purpose |
|---|---|---|---|
| MinIO | `minio/` | 9000 (API), 9001 (console) | Object storage — DVC remote, raw data |
| Kafka (3-broker) | `kafka/` | 9092, 9192, 9292 | Event streaming (Kafka ingestion pipeline) |
| Redis | (standalone) | 6379 | Feast online feature store |

All services that need to communicate share the `disease-trend-network` Docker network.

---

## Setup

### 1. Create the shared Docker network (once)
```bash
docker network create disease-trend-network
```

### 2. Start MinIO
```bash
cd minio/
docker compose up -d
```

MinIO will auto-create two buckets on first start:
- `disease-raw` — raw ingested data
- `disease-processed` — DVC-tracked data (Bronze/Silver/Gold parquets)

Access the MinIO console at [http://localhost:9001](http://localhost:9001)
- Username: `minioadmin`
- Password: `minioadmin`

### 3. Start Kafka
```bash
cd kafka/
docker compose up -d
```

Verify all 3 brokers are running:
```bash
docker compose ps
```

### 4. Start Redis
Redis is run as a standalone container (not in a compose file yet):
```bash
docker run -d --name redis -p 6379:6379 redis:7
```

---

## Kafka Cluster Details

3-broker KRaft cluster (no Zookeeper). Each broker acts as both controller and broker.

| Broker | Container | External port | Internal address |
|---|---|---|---|
| broker-1 | `broker-1` | `localhost:9092` | `broker-1:9094` |
| broker-2 | `broker-2` | `localhost:9192` | `broker-2:9094` |
| broker-3 | `broker-3` | `localhost:9292` | `broker-3:9094` |

**Connecting from outside Docker:** use `localhost:9092` (or 9192/9292)  
**Connecting from inside Docker:** use `broker-1:9094` (INTERNAL listener)

Cluster ID is fixed at `1` — do not change this after the volumes are initialized.
To reset the cluster, remove volumes: `docker compose down -v && docker compose up -d`

---

## Stopping Services

```bash
# Stop MinIO
cd minio/ && docker compose down

# Stop Kafka
cd kafka/ && docker compose down

# Stop Redis
docker stop redis

# Stop everything and remove volumes (destructive — loses all data)
cd minio/ && docker compose down -v
cd kafka/ && docker compose down -v
docker rm -f redis
```

---

## Troubleshooting

**MinIO: `network disease-trend-network not found`**
→ Run `docker network create disease-trend-network` first.

**Kafka: brokers not forming a cluster**
→ The `CLUSTER_ID` must be identical across all brokers. Check it is set to `1` in
all three broker environment configs. If volumes are corrupted, do a full reset:
`docker compose down -v && docker compose up -d`

**Kafka: can't connect from host machine**
→ Use `localhost:9092` (not `broker-1:9092`). The `PLAINTEXT` listener is advertised
on `localhost` for external access.

**Redis: connection refused**
→ Run `docker ps | grep redis` to check if the container is running.
If not: `docker run -d --name redis -p 6379:6379 redis:7`
