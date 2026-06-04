"""Generate all pipeline diagrams for the IEEE paper."""
import os
import textwrap

OUT = os.path.dirname(__file__)

# ── Shared style constants ─────────────────────────────────────────────────
FONT      = "Arial, Helvetica, sans-serif"
MONO      = "Courier New, Courier, monospace"
C_PROC    = "#f2f2f2"   # component / process
C_STORE   = "#fffde7"   # storage (BQ, GCS, Redis)
C_SVC     = "#e8f4f8"   # service (FastAPI, Kafka, Feast)
C_EXT     = "#fce4ec"   # external / user-facing
C_GROUP   = "#f9f9f9"   # swimlane background
STROKE    = "#2a2a2a"
STROKE_LT = "#888888"
STROKE_GRP= "#aaaaaa"
TEXT_MAIN = "#111111"
TEXT_SUB  = "#555555"
TEXT_GRP  = "#777777"

ARROW_DEF = """\
<defs>
  <marker id="arr" markerWidth="9" markerHeight="7"
          refX="8" refY="3.5" orient="auto">
    <polygon points="0 0,9 3.5,0 7" fill="#2a2a2a"/>
  </marker>
  <marker id="arr-lt" markerWidth="9" markerHeight="7"
          refX="8" refY="3.5" orient="auto">
    <polygon points="0 0,9 3.5,0 7" fill="#888888"/>
  </marker>
  <marker id="arr-blue" markerWidth="9" markerHeight="7"
          refX="8" refY="3.5" orient="auto">
    <polygon points="0 0,9 3.5,0 7" fill="#1565c0"/>
  </marker>
</defs>"""


def svg_open(w, h):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="0 0 {w} {h}" '
            f'font-family="{FONT}" '
            f'style="background:#fff">\n')

def svg_close():
    return "</svg>\n"

def rect(x, y, w, h, fill=C_PROC, stroke=STROKE, sw=1.5, rx=5, opacity=1):
    return (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}" '
            f'opacity="{opacity}"/>\n')

def text(x, y, s, size=11, weight="normal", fill=TEXT_MAIN,
         anchor="middle", family=None, dy=0):
    fam = f' font-family="{family}"' if family else ""
    return (f'<text x="{x}" y="{y+dy}" text-anchor="{anchor}" '
            f'font-size="{size}" font-weight="{weight}" '
            f'fill="{fill}"{fam}>{s}</text>\n')

def mtext(x, y, lines, size=10, fill=TEXT_SUB, anchor="middle", lh=14):
    """Multi-line text block."""
    out = ""
    for i, line in enumerate(lines):
        out += text(x, y + i * lh, line, size=size, fill=fill, anchor=anchor)
    return out

def arrow(x1, y1, x2, y2, dash=False, color="arr", sw=1.5):
    da = ' stroke-dasharray="6,3"' if dash else ""
    col = {"arr": STROKE, "arr-lt": STROKE_LT, "arr-blue": "#1565c0"}[color]
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
            f'stroke="{col}" stroke-width="{sw}"{da} '
            f'marker-end="url(#{color})"/>\n')

def path_arrow(d, dash=False, color="arr", sw=1.5):
    da = ' stroke-dasharray="6,3"' if dash else ""
    col = {"arr": STROKE, "arr-lt": STROKE_LT, "arr-blue": "#1565c0"}[color]
    return (f'<path d="{d}" fill="none" stroke="{col}" '
            f'stroke-width="{sw}"{da} '
            f'marker-end="url(#{color})"/>\n')

def label_on_arrow(x, y, s, size=9, fill=TEXT_SUB):
    return (f'<text x="{x}" y="{y}" text-anchor="middle" '
            f'font-size="{size}" fill="{fill}" '
            f'style="font-style:italic">{s}</text>\n')

def group_box(x, y, w, h, title="", title_size=10):
    out = (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" '
           f'fill="{C_GROUP}" stroke="{STROKE_GRP}" stroke-width="1" '
           f'stroke-dasharray="6,3"/>\n')
    if title:
        out += (f'<text x="{x+8}" y="{y+13}" font-size="{title_size}" '
                f'font-weight="bold" fill="{TEXT_GRP}" '
                f'text-transform="uppercase">{title}</text>\n')
    return out

def badge(x, y, w, h, fill, stroke, title, sub=None):
    out = rect(x, y, w, h, fill=fill, stroke=stroke, rx=4)
    if sub:
        out += text(x + w//2, y + h//2 - 7, title, size=11,
                    weight="bold", fill=TEXT_MAIN)
        out += text(x + w//2, y + h//2 + 7, sub, size=9,
                    fill=TEXT_SUB, family=MONO)
    else:
        out += text(x + w//2, y + h//2 + 4, title, size=11,
                    weight="bold", fill=TEXT_MAIN)
    return out

def cylinder(x, y, w, h, fill=C_STORE, stroke=STROKE):
    """Pseudo-cylinder for database/storage."""
    ry = 10
    out  = f'<ellipse cx="{x+w//2}" cy="{y+ry}" rx="{w//2}" ry="{ry}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>\n'
    out += f'<rect x="{x}" y="{y+ry}" width="{w}" height="{h-ry}" fill="{fill}" stroke="none"/>\n'
    out += f'<line x1="{x}" y1="{y+ry}" x2="{x}" y2="{y+h}" stroke="{stroke}" stroke-width="1.5"/>\n'
    out += f'<line x1="{x+w}" y1="{y+ry}" x2="{x+w}" y2="{y+h}" stroke="{stroke}" stroke-width="1.5"/>\n'
    out += f'<ellipse cx="{x+w//2}" cy="{y+h}" rx="{w//2}" ry="{ry}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>\n'
    return out

def hr(x1, x2, y, stroke=STROKE_GRP, dash=True):
    da = ' stroke-dasharray="4,3"' if dash else ""
    return f'<line x1="{x1}" y1="{y}" x2="{x2}" y2="{y}" stroke="{stroke}" stroke-width="0.8"{da}/>\n'

# ══════════════════════════════════════════════════════════════════════════════
# DIAGRAM 1 — Overall Architecture
# ══════════════════════════════════════════════════════════════════════════════
def diag_overall():
    W, H = 820, 660
    out = svg_open(W, H) + ARROW_DEF

    # Title
    out += text(W//2, 26, "Overall System Architecture",
                size=15, weight="bold", fill="#111")
    out += text(W//2, 42, "KKBox Churn Prediction MLOps Pipeline",
                size=10, fill=TEXT_SUB)

    # ── Zone A: Data Sources ───────────────────────────────────────────────
    out += group_box(20, 56, 780, 72, "① Data Sources")
    # GCS Bronze
    out += rect(36, 72, 148, 44, fill="#fff8e1", stroke="#c8a200")
    out += text(110, 88, "GCS Bronze", size=11, weight="bold")
    out += text(110, 103, "user_logs_v2 · transactions_v2", size=9, fill=TEXT_SUB)
    # BQ features_train
    out += rect(224, 72, 168, 44, fill="#fff8e1", stroke="#c8a200")
    out += text(308, 88, "BigQuery Gold", size=11, weight="bold")
    out += text(308, 103, "features_train  (~1M rows)", size=9, fill=TEXT_SUB)
    # Kaggle label
    out += text(490, 88, "Source: WSDM KKBox Cup 2018", size=9, fill="#999", anchor="start")

    # ── Zone B: Data Pipeline ─────────────────────────────────────────────
    out += group_box(20, 148, 380, 178, "② Data Pipeline")
    # Producer
    out += rect(36, 164, 148, 40, fill=C_PROC, stroke=STROKE)
    out += text(110, 180, "kafka_producer.py", size=11, weight="bold")
    out += text(110, 193, "GCS → Kafka (zero disk)", size=9, fill=TEXT_SUB)
    # Kafka
    out += rect(36, 224, 348, 40, fill=C_SVC, stroke="#1565c0")
    out += text(210, 240, "Apache Kafka  (KRaft, no ZooKeeper)", size=11, weight="bold")
    out += text(210, 253, "kkbox.user_logs  ·  kkbox.transactions  +  EOD markers", size=9, fill=TEXT_SUB)
    # Consumer
    out += rect(36, 284, 348, 40, fill=C_PROC, stroke=STROKE)
    out += text(210, 300, "kafka_consumer.py", size=11, weight="bold")
    out += text(210, 313, "cumulative aggregation · dual EOD · background I/O thread", size=9, fill=TEXT_SUB)

    # ── Zone C: Feature Store ─────────────────────────────────────────────
    out += group_box(20, 346, 380, 120, "③ Feature Store")
    out += rect(36, 362, 148, 40, fill="#fff8e1", stroke="#c8a200")
    out += text(110, 378, "BigQuery Gold", size=11, weight="bold")
    out += text(110, 391, "features_streaming (append)", size=9, fill=TEXT_SUB)
    out += rect(224, 362, 160, 40, fill="#e8f5e9", stroke="#388e3c")
    out += text(304, 378, "Redis (Feast)", size=11, weight="bold")
    out += text(304, 391, "Cloud Memorystore :6379", size=9, fill=TEXT_SUB)
    out += rect(36, 412, 348, 44, fill=C_SVC, stroke="#1565c0")
    out += text(210, 428, "Feast Feature Store", size=11, weight="bold")
    out += text(210, 443, "offline: BigQuery  ·  online: Redis  ·  entity: msno", size=9, fill=TEXT_SUB)

    # ── Zone D: Model Pipeline ────────────────────────────────────────────
    out += group_box(430, 148, 370, 130, "④ Model Pipeline")
    out += rect(446, 164, 160, 40, fill=C_PROC, stroke=STROKE)
    out += text(526, 180, "train.py", size=11, weight="bold")
    out += text(526, 193, "XGBoost · out-of-time split", size=9, fill=TEXT_SUB)
    out += rect(446, 216, 100, 40, fill=C_SVC, stroke="#6a1b9a")
    out += text(496, 232, "MLflow", size=11, weight="bold")
    out += text(496, 245, "experiment log", size=9, fill=TEXT_SUB)
    out += rect(564, 216, 152, 40, fill="#fff8e1", stroke="#c8a200")
    out += text(640, 232, "GCS Artifacts", size=11, weight="bold")
    out += text(640, 245, "model.ubj · config.json", size=9, fill=TEXT_SUB)
    out += rect(446, 266, 270, 12, fill="none", stroke="none")

    # ── Zone E: Serving Pipeline ──────────────────────────────────────────
    out += group_box(430, 298, 370, 168, "⑤ Serving Pipeline")
    out += rect(446, 314, 338, 44, fill=C_SVC, stroke="#0d47a1", sw=2)
    out += text(615, 330, "FastAPI  :8000", size=12, weight="bold")
    out += text(615, 347, "PredictionService · StreamingController · SSE", size=9, fill=TEXT_SUB)
    out += rect(446, 368, 150, 36, fill=C_PROC, stroke=STROKE)
    out += text(521, 384, "nginx  :80", size=11, weight="bold")
    out += text(521, 397, "reverse proxy", size=9, fill=TEXT_SUB)
    out += rect(610, 368, 174, 36, fill=C_EXT, stroke="#880e4f")
    out += text(697, 384, "React Dashboard  /ui/", size=11, weight="bold")
    out += text(697, 397, "6 pages · CDN · SSE client", size=9, fill=TEXT_SUB)
    out += rect(446, 416, 338, 36, fill="#f3f3f3", stroke=STROKE_LT)
    out += text(615, 432, "GCE VM  e2-standard-2  ·  asia-southeast1-b", size=9, fill=TEXT_SUB)
    out += text(615, 445, "Static IP: 35.198.232.66  ·  systemd kkbox-serving", size=9, fill=TEXT_SUB)

    # ── Zone F: Monitoring ────────────────────────────────────────────────
    out += group_box(430, 486, 370, 104, "⑥ Monitoring Pipeline")
    out += rect(446, 502, 160, 36, fill=C_PROC, stroke="#e65100")
    out += text(526, 518, "Prometheus  :9090", size=11, weight="bold")
    out += text(526, 531, "scrape /metrics every 15s", size=9, fill=TEXT_SUB)
    out += rect(622, 502, 162, 36, fill=C_PROC, stroke="#e65100")
    out += text(703, 518, "Grafana  :3000", size=11, weight="bold")
    out += text(703, 531, "API Health · Churn dashboards", size=9, fill=TEXT_SUB)
    out += text(615, 564, "Docker  (monitoring_pipeline/docker-compose.yml)", size=9, fill=TEXT_GRP)

    # ── Users ─────────────────────────────────────────────────────────────
    out += rect(430, 608, 370, 36, fill="#e8f5e9", stroke="#2e7d32", sw=2)
    out += text(615, 630, "Users  /  Customer Success Team", size=12, weight="bold", fill="#1b5e20")

    # ── Arrows ────────────────────────────────────────────────────────────
    # Sources → Producer / BQ
    out += arrow(110, 116, 110, 164)
    out += arrow(308, 116, 308, 284, dash=True, color="arr-lt")
    label_on = label_on_arrow(340, 210, "baseline features")
    out += label_on

    # Producer → Kafka
    out += arrow(110, 204, 110, 224)

    # Kafka → Consumer
    out += arrow(210, 264, 210, 284)

    # Consumer → BQ streaming
    out += arrow(110, 324, 110, 362)

    # Consumer → Redis (via Feast)
    out += arrow(210, 324, 210, 412)

    # BQ streaming → Feast
    out += arrow(110, 402, 110, 412)

    # Feast → Redis
    out += arrow(278, 432, 278, 362, dash=False)

    # BQ features_train → train.py
    out += arrow(308, 116, 526, 164, dash=True, color="arr-lt")

    # train.py → MLflow
    out += arrow(496, 204, 496, 216)

    # train.py → GCS artifacts
    out += arrow(606, 196, 640, 216)

    # GCS artifacts → FastAPI (load)
    out += arrow(640, 256, 640, 314, dash=True, color="arr-lt")

    # Redis → FastAPI (Feast lookup)
    out += path_arrow(f"M 400 378 L 446 338", color="arr-lt")
    out += label_on_arrow(425, 355, "online features")

    # FastAPI → nginx
    out += arrow(526, 358, 521, 368)

    # FastAPI → React
    out += arrow(697, 358, 697, 368, dash=True, color="arr-blue")
    out += label_on_arrow(720, 363, "SSE")

    # nginx → Users
    out += path_arrow(f"M 521 404 L 521 608", color="arr")
    out += label_on_arrow(530, 520, "HTTP :80")

    # FastAPI → Prometheus
    out += path_arrow(f"M 615 358 L 526 502", dash=True, color="arr-lt")
    out += label_on_arrow(548, 440, "/metrics")

    # Prometheus → Grafana
    out += arrow(606, 520, 622, 520)

    out += svg_close()
    return out


# ══════════════════════════════════════════════════════════════════════════════
# DIAGRAM 2 — Data Pipeline (detailed)
# ══════════════════════════════════════════════════════════════════════════════
def diag_data():
    W, H = 800, 950
    out = svg_open(W, H) + ARROW_DEF

    out += text(W//2, 26, "Data Pipeline — Detailed Architecture",
                size=14, weight="bold")
    out += text(W//2, 42, "Historical Playback Streaming: GCS Bronze → Kafka → Feature Store",
                size=10, fill=TEXT_SUB)

    # ── GCS Bronze ────────────────────────────────────────────────────────
    out += group_box(20, 52, 760, 60, "GCS Bronze Layer")
    out += rect(36, 68, 194, 36, fill="#fff8e1", stroke="#c8a200")
    out += text(133, 82, "user_logs_v2.csv", size=11, weight="bold")
    out += text(133, 94, "1.3 GB  ·  2017 daily listening logs", size=9, fill=TEXT_SUB)
    out += rect(246, 68, 194, 36, fill="#fff8e1", stroke="#c8a200")
    out += text(343, 82, "transactions_v2.csv", size=11, weight="bold")
    out += text(343, 94, "110 MB  ·  2017 subscription transactions", size=9, fill=TEXT_SUB)
    out += rect(456, 68, 300, 36, fill="#fff8e1", stroke="#c8a200")
    out += text(606, 82, "features_train  (BigQuery Gold)", size=11, weight="bold")
    out += text(606, 94, "~1M rows  ·  snapshot ≤ 2016-12-31  ·  baseline", size=9, fill=TEXT_SUB)

    # ── Producer ──────────────────────────────────────────────────────────
    out += group_box(20, 130, 380, 220, "kafka_producer.py")
    # Step 1: pre-load
    out += rect(36, 146, 348, 48, fill=C_PROC, stroke=STROKE)
    out += text(210, 160, "① Pre-load Transactions", size=11, weight="bold")
    out += text(210, 175, "pd.read_csv(GCS_URL, chunksize=50k)  →  txns_by_date dict", size=9, fill=TEXT_SUB, family=MONO)
    out += text(210, 187, "~25 MB in-memory  ·  only 2017 rows kept", size=9, fill=TEXT_SUB)

    # Step 2: stream user logs
    out += rect(36, 204, 348, 48, fill=C_PROC, stroke=STROKE)
    out += text(210, 218, "② Stream user_logs in 200k-row Chunks", size=11, weight="bold")
    out += text(210, 231, "gcsfs  →  no local disk write  ·  dtype=int32/float32", size=9, fill=TEXT_SUB, family=MONO)
    out += text(210, 243, "groupby date  →  accumulate ul_parts[d]", size=9, fill=TEXT_SUB)

    # Step 3: send day
    out += rect(36, 262, 348, 56, fill=C_PROC, stroke=STROKE)
    out += text(210, 276, "③ Per-day Send", size=11, weight="bold")
    out += text(210, 291, "send user_logs → TOPIC_USER_LOGS", size=9, fill=TEXT_SUB, family=MONO)
    out += text(210, 303, "send transactions → TOPIC_TRANSACTIONS", size=9, fill=TEXT_SUB, family=MONO)
    out += text(210, 315, "flush()  →  EOD marker on BOTH topics", size=9, fill="#c62828", weight="bold")

    # step 4 sleep
    out += rect(36, 328, 348, 14, fill="#e8e8e8", stroke=STROKE_LT)
    out += text(210, 338, "sleep(speed)  ·  default 55 s/day", size=9, fill=TEXT_GRP)

    # ── EOD marker detail ─────────────────────────────────────────────────
    out += group_box(420, 130, 360, 90, "EOD Marker (Dual-Topic)")
    out += rect(436, 148, 328, 64, fill="#fff3e0", stroke="#e65100")
    out += text(600, 163, "eod = {\"_end_of_day\": True, \"date\": d}", size=9, fill="#333", family=MONO)
    out += text(600, 177, "send(TOPIC_USER_LOGS,    \"__eod__\", eod)", size=9, fill="#bf360c", family=MONO, weight="bold")
    out += text(600, 191, "send(TOPIC_TRANSACTIONS, \"__eod__\", eod)", size=9, fill="#bf360c", family=MONO, weight="bold")
    out += text(600, 205, "producer.flush()      # guarantees delivery", size=9, fill="#555", family=MONO)

    # ── Kafka Topics ──────────────────────────────────────────────────────
    out += group_box(20, 368, 760, 68, "Kafka Topics  (KRaft Mode — No ZooKeeper)")
    out += rect(36, 384, 348, 44, fill=C_SVC, stroke="#1565c0", sw=2)
    out += text(210, 400, "kkbox.user_logs", size=12, weight="bold", fill="#0d47a1")
    out += text(210, 415, "key = msno  ·  daily listening activity rows", size=9, fill=TEXT_SUB)
    out += rect(400, 384, 364, 44, fill=C_SVC, stroke="#1565c0", sw=2)
    out += text(582, 400, "kkbox.transactions", size=12, weight="bold", fill="#0d47a1")
    out += text(582, 415, "key = msno  ·  subscription transaction rows  +  EOD", size=9, fill=TEXT_SUB)

    # ── Consumer internal ─────────────────────────────────────────────────
    out += group_box(20, 454, 760, 336, "kafka_consumer.py")

    # Startup steps
    out += rect(36, 470, 220, 44, fill=C_PROC, stroke=STROKE)
    out += text(146, 485, "① Lock Kafka Offset", size=11, weight="bold")
    out += text(146, 498, "auto_offset_reset=latest", size=9, fill=TEXT_SUB, family=MONO)
    out += text(146, 509, "consumer.poll(10s)  →  offset committed", size=9, fill=TEXT_SUB)

    out += rect(272, 470, 220, 44, fill=C_PROC, stroke=STROKE)
    out += text(382, 485, "② Load Baseline  (20s delay)", size=11, weight="bold")
    out += text(382, 498, "BQ features_train → members dict", size=9, fill=TEXT_SUB, family=MONO)
    out += text(382, 509, "~1M msno  ·  16 numeric features each", size=9, fill=TEXT_SUB)

    out += rect(508, 470, 256, 44, fill=C_PROC, stroke=STROKE)
    out += text(636, 485, "③ Consume Loop", size=11, weight="bold")
    out += text(636, 498, "for msg in consumer:", size=9, fill=TEXT_SUB, family=MONO)
    out += text(636, 509, "buffer by (topic, date, msno)", size=9, fill=TEXT_SUB)

    # Dual EOD box
    out += rect(36, 534, 460, 64, fill="#fff3e0", stroke="#e65100")
    out += text(266, 548, "④ Dual EOD Tracking", size=11, weight="bold", fill="#bf360c")
    out += text(266, 562, "eod_logs.add(date)  /  eod_txns.add(date)", size=9, fill="#555", family=MONO)
    out += text(266, 574, "if date in eod_logs AND date in eod_txns:", size=9, fill="#555", family=MONO)
    out += text(266, 586, "    flush(date)       # prevents partial flush", size=9, fill="#c62828", weight="bold", family=MONO)

    # Cumulative aggregation box
    out += rect(36, 618, 460, 68, fill="#e8f5e9", stroke="#2e7d32")
    out += text(266, 632, "⑤ Cumulative Feature Aggregation", size=11, weight="bold", fill="#1b5e20")
    out += text(266, 647, "x_t(u) = x_0(u)  +  Σ Δx_d(u),  d=1..t", size=10, fill="#2e7d32", family=MONO)
    out += text(266, 661, "baseline (BQ features_train)  +  streaming delta", size=9, fill=TEXT_SUB)
    out += text(266, 673, "members.update(updates)   # immediate, before BQ", size=9, fill="#555", family=MONO)

    # Flush box
    out += rect(36, 696, 222, 80, fill=C_PROC, stroke=STROKE)
    out += text(147, 711, "⑥ flush(date)", size=11, weight="bold")
    out += text(147, 726, "POST /stream/notify", size=9, fill="#1565c0", weight="bold", family=MONO)
    out += text(147, 738, "(non-blocking, before I/O)", size=9, fill=TEXT_SUB)
    out += text(147, 752, "build_rows() → rows, updates", size=9, fill=TEXT_SUB, family=MONO)
    out += text(147, 764, "bg_queue.put((date, rows))", size=9, fill=TEXT_SUB, family=MONO)

    # Background thread
    out += rect(274, 696, 222, 80, fill="#f3e5f5", stroke="#6a1b9a")
    out += text(385, 711, "⑦ Background Thread", size=11, weight="bold", fill="#4a148c")
    out += text(385, 726, "write_to_bq(rows)  ~30-60s", size=9, fill=TEXT_SUB, family=MONO)
    out += text(385, 738, "feast.materialize(date)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(385, 752, "bg_queue.task_done()", size=9, fill=TEXT_SUB, family=MONO)
    out += text(385, 764, "bg_queue.join()  # clean shutdown", size=9, fill=TEXT_SUB, family=MONO)

    # Stale cleanup
    out += rect(512, 696, 248, 80, fill=C_PROC, stroke=STROKE)
    out += text(636, 711, "Auto Cleanup on /stream/start", size=11, weight="bold")
    out += text(636, 726, "pkill -f kafka_producer.py", size=9, fill="#333", family=MONO)
    out += text(636, 738, "pkill -f kafka_consumer.py", size=9, fill="#333", family=MONO)
    out += text(636, 752, "kills tracked PIDs (SIGTERM)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(636, 764, "Ensures clean start each time", size=9, fill=TEXT_SUB)

    # ── Outputs ───────────────────────────────────────────────────────────
    out += group_box(20, 808, 760, 68, "Output — Feature Store")
    out += rect(36, 824, 320, 44, fill="#fff8e1", stroke="#c8a200")
    out += text(196, 839, "BigQuery  kkbox_gold.features_streaming", size=11, weight="bold")
    out += text(196, 852, "append-only  ·  partitioned by event_timestamp", size=9, fill=TEXT_SUB)
    out += rect(372, 824, 392, 44, fill="#e8f5e9", stroke="#388e3c")
    out += text(568, 839, "Redis  (Cloud Memorystore 10.80.68.19:6379)", size=11, weight="bold")
    out += text(568, 852, "latest cumulative features per msno  ·  Feast online store", size=9, fill=TEXT_SUB)

    # ── Arrows ────────────────────────────────────────────────────────────
    out += arrow(133, 104, 133, 146)
    out += arrow(343, 104, 343, 204)
    out += arrow(210, 342, 210, 384)
    out += arrow(582, 220, 582, 384)
    out += arrow(210, 428, 210, 454)
    out += arrow(582, 428, 582, 454)
    out += arrow(146, 514, 146, 534)
    out += arrow(382, 514, 266, 534)
    out += arrow(636, 514, 460, 560)
    out += arrow(266, 598, 266, 618)
    out += arrow(266, 686, 147, 696)
    out += arrow(266, 686, 385, 696)
    out += arrow(147, 776, 196, 824)
    out += arrow(385, 776, 568, 824)

    # BQ baseline → consumer startup
    out += path_arrow(f"M 606, 104 L 636, 470", dash=True, color="arr-lt")
    out += label_on_arrow(640, 290, "baseline load")

    # notify → FastAPI
    out += path_arrow(f"M 180, 726 L 790, 726 L 790, 888 L 764, 846",
                      dash=True, color="arr-blue")
    out += label_on_arrow(793, 800, "POST /notify", fill="#1565c0")

    out += svg_close()
    return out


# ══════════════════════════════════════════════════════════════════════════════
# DIAGRAM 3 — Model Pipeline (detailed)
# ══════════════════════════════════════════════════════════════════════════════
def diag_model():
    W, H = 780, 700
    out = svg_open(W, H) + ARROW_DEF

    out += text(W//2, 26, "Model Pipeline — Detailed Architecture",
                size=14, weight="bold")
    out += text(W//2, 42, "XGBoost Training with Out-of-Time Validation · MLflow Tracking · GCS Artifact Upload",
                size=10, fill=TEXT_SUB)

    # ── Data Source ───────────────────────────────────────────────────────
    out += group_box(20, 52, 740, 60, "Data Source")
    out += rect(36, 68, 352, 36, fill="#fff8e1", stroke="#c8a200")
    out += text(212, 82, "BigQuery  kkbox_gold.features_train", size=11, weight="bold")
    out += text(212, 95, "~1,082,190 rows  ·  18 features  ·  is_churn label  ·  churn rate ≈ 10%", size=9, fill=TEXT_SUB)
    out += rect(404, 68, 340, 36, fill="#f3f3f3", stroke=STROKE_LT)
    out += text(574, 82, "pandas_gbq.read_gbq(BQ_TABLE, project_id)", size=9, fill="#333", family=MONO)
    out += text(574, 95, "→ pd.DataFrame  (all 18 feature cols + target + metadata)", size=9, fill=TEXT_SUB)

    # ── Preprocessing ─────────────────────────────────────────────────────
    out += group_box(20, 130, 740, 120, "Data Preprocessing")
    out += rect(36, 148, 200, 90, fill=C_PROC, stroke=STROKE)
    out += text(136, 164, "Numeric Features", size=11, weight="bold")
    out += text(136, 179, "fillna(0)  for NUM_COLS", size=9, fill=TEXT_SUB, family=MONO)
    out += text(136, 193, "14 cols: total_secs,", size=9, fill=TEXT_SUB)
    out += text(136, 205, "cancel_count, etc.", size=9, fill=TEXT_SUB)
    out += text(136, 217, "medians computed here", size=9, fill="#1b5e20", weight="bold")

    out += rect(252, 148, 200, 90, fill=C_PROC, stroke=STROKE)
    out += text(352, 164, "Categorical Features", size=11, weight="bold")
    out += text(352, 179, "gender → {male:0, female:1,", size=9, fill=TEXT_SUB, family=MONO)
    out += text(352, 191, "          unknown:2}", size=9, fill=TEXT_SUB, family=MONO)
    out += text(352, 205, "bd → fillna(bd_median)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(352, 217, "city → fillna(city_mode)", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(468, 148, 276, 90, fill="#e8f5e9", stroke="#2e7d32")
    out += text(606, 164, "preprocessing_config.json", size=11, weight="bold", fill="#1b5e20")
    out += text(606, 179, "num_cols_medians: {col: median}", size=9, fill=TEXT_SUB, family=MONO)
    out += text(606, 191, "bd_median, city_mode,", size=9, fill=TEXT_SUB, family=MONO)
    out += text(606, 203, "registered_via_mode,", size=9, fill=TEXT_SUB, family=MONO)
    out += text(606, 215, "gender_map: {male:0, ...}", size=9, fill=TEXT_SUB, family=MONO)
    out += text(606, 227, "→ saved to GCS for serving", size=9, fill="#2e7d32", weight="bold")

    # ── Out-of-Time Split ─────────────────────────────────────────────────
    out += group_box(20, 268, 740, 84, "Out-of-Time Train / Test Split")
    out += rect(36, 284, 348, 56, fill=C_PROC, stroke=STROKE)
    out += text(210, 299, "Train Set  (~804k rows)", size=11, weight="bold")
    out += text(210, 313, "registration_init_time < 2016-06-01", size=9, fill=TEXT_SUB, family=MONO)
    out += text(210, 327, "churn rate ≈ 10%  ·  features + is_churn", size=9, fill=TEXT_SUB)
    out += rect(400, 284, 344, 56, fill=C_PROC, stroke=STROKE)
    out += text(572, 299, "Test Set  (~157k rows)", size=11, weight="bold")
    out += text(572, 313, "registration_init_time ≥ 2016-06-01", size=9, fill=TEXT_SUB, family=MONO)
    out += text(572, 327, "churn rate ≈ 10%  ·  held-out, no leakage", size=9, fill=TEXT_SUB)

    # ── XGBoost Training ──────────────────────────────────────────────────
    out += group_box(20, 370, 740, 120, "XGBoost Training  (model.fit)")
    out += rect(36, 388, 220, 90, fill=C_PROC, stroke=STROKE)
    out += text(146, 403, "Hyperparameters", size=11, weight="bold")
    out += text(146, 418, "n_estimators=500", size=9, fill=TEXT_SUB, family=MONO)
    out += text(146, 430, "max_depth=6  ·  lr=0.05", size=9, fill=TEXT_SUB, family=MONO)
    out += text(146, 442, "subsample=0.8  ·  colsample=0.8", size=9, fill=TEXT_SUB, family=MONO)
    out += text(146, 454, "min_child_weight=10", size=9, fill=TEXT_SUB, family=MONO)
    out += text(146, 466, "early_stopping_rounds=30", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(272, 388, 220, 90, fill="#fff3e0", stroke="#e65100")
    out += text(382, 403, "Class Imbalance Handling", size=11, weight="bold")
    out += text(382, 418, "scale_pos_weight =", size=9, fill=TEXT_SUB, family=MONO)
    out += text(382, 430, "  |neg| / |pos| ≈ 9.0", size=9, fill="#bf360c", weight="bold", family=MONO)
    out += text(382, 444, "eval_metric = AUC", size=9, fill=TEXT_SUB, family=MONO)
    out += text(382, 456, "eval_set = [(X_test, y_test)]", size=9, fill=TEXT_SUB, family=MONO)
    out += text(382, 468, "verbose every 50 rounds", size=9, fill=TEXT_SUB)

    out += rect(508, 388, 236, 90, fill=C_PROC, stroke=STROKE)
    out += text(626, 403, "Threshold Optimization", size=11, weight="bold")
    out += text(626, 418, "τ̂ = argmax_τ  F₁(τ)", size=9, fill="#333")
    out += text(626, 432, "PR curve → best_idx", size=9, fill=TEXT_SUB, family=MONO)
    out += text(626, 444, "best_threshold = 0.789", size=9, fill="#1b5e20", weight="bold")
    out += text(626, 456, "serving default = 0.781", size=9, fill=TEXT_SUB)
    out += text(626, 468, "override via CHURN_THRESHOLD", size=9, fill=TEXT_SUB, family=MONO)

    # ── Evaluation ────────────────────────────────────────────────────────
    out += group_box(20, 508, 740, 64, "Evaluation Metrics  (on Test Set)")
    out += rect(36, 524, 104, 36, fill="#e8f5e9", stroke="#388e3c")
    out += text(88, 537, "AUC-ROC", size=10, weight="bold", fill="#1b5e20")
    out += text(88, 549, "0.8924", size=11, weight="bold", fill="#1b5e20")
    out += rect(152, 524, 104, 36, fill="#e8f5e9", stroke="#388e3c")
    out += text(204, 537, "AUC-PR", size=10, weight="bold", fill="#1b5e20")
    out += text(204, 549, "0.5044", size=11, weight="bold", fill="#1b5e20")
    out += rect(268, 524, 104, 36, fill=C_PROC, stroke=STROKE_LT)
    out += text(320, 537, "F1-Score", size=10, weight="bold")
    out += text(320, 549, "0.5068", size=11, weight="bold")
    out += rect(384, 524, 104, 36, fill=C_PROC, stroke=STROKE_LT)
    out += text(436, 537, "Precision", size=10, weight="bold")
    out += text(436, 549, "0.3593", size=11, weight="bold")
    out += rect(500, 524, 104, 36, fill=C_PROC, stroke=STROKE_LT)
    out += text(552, 537, "Recall", size=10, weight="bold")
    out += text(552, 549, "0.8596", size=11, weight="bold")
    out += rect(616, 524, 128, 36, fill=C_PROC, stroke=STROKE_LT)
    out += text(680, 537, "Log Loss", size=10, weight="bold")
    out += text(680, 549, "0.2341", size=11, weight="bold")

    # ── Artifact Upload ───────────────────────────────────────────────────
    out += group_box(20, 590, 740, 96, "Artifact Storage")
    out += rect(36, 608, 220, 68, fill="#f3e5f5", stroke="#6a1b9a")
    out += text(146, 623, "MLflow  (experiment tracking)", size=11, weight="bold", fill="#4a148c")
    out += text(146, 638, "log_params(hyperparams)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(146, 650, "log_metrics(auc, f1, ...)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(146, 662, "log_model(model, registered_name)", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(272, 608, 472, 68, fill="#fff8e1", stroke="#c8a200")
    out += text(508, 623, "GCS  gs://kkbox-churn-prediction-493716-data/models/kkbox-churn-xgboost/", size=9, fill=TEXT_SUB, family=MONO)
    out += text(400, 638, "model.ubj", size=10, weight="bold")
    out += text(508, 638, "preprocessing_config.json", size=10, weight="bold")
    out += text(640, 638, "feature_cols.json", size=10, weight="bold")
    out += text(400, 652, "XGBoost binary", size=9, fill=TEXT_SUB)
    out += text(508, 652, "medians · modes · gender_map", size=9, fill=TEXT_SUB)
    out += text(640, 652, "18 feature names ordered", size=9, fill=TEXT_SUB)
    out += text(400, 664, "saved via tempfile upload", size=9, fill=TEXT_GRP, family=MONO)
    out += text(508, 664, "cold-start imputation values", size=9, fill=TEXT_GRP)
    out += text(640, 664, "Feast + model must agree", size=9, fill=TEXT_GRP)

    # ── Arrows ────────────────────────────────────────────────────────────
    out += arrow(212, 104, 212, 148)
    out += arrow(606, 104, 606, 148)
    out += arrow(210, 238, 210, 284)
    out += arrow(572, 238, 572, 284)
    out += arrow(210, 340, 210, 388)
    out += arrow(572, 340, 572, 388)
    out += arrow(382, 478, 382, 524)
    out += arrow(210, 478, 146, 608)
    out += arrow(382, 478, 508, 608)
    out += arrow(606, 240, 606, 508, dash=True, color="arr-lt")

    out += svg_close()
    return out


# ══════════════════════════════════════════════════════════════════════════════
# DIAGRAM 4 — Serving Pipeline (detailed)
# ══════════════════════════════════════════════════════════════════════════════
def diag_serving():
    W, H = 800, 860
    out = svg_open(W, H) + ARROW_DEF

    out += text(W//2, 26, "Serving Pipeline — Detailed Architecture",
                size=14, weight="bold")
    out += text(W//2, 42, "Server Startup · Online Prediction · Streaming Simulation Controller",
                size=10, fill=TEXT_SUB)

    # ── Flow A: Startup ───────────────────────────────────────────────────
    out += group_box(20, 52, 760, 140, "Flow A — Server Startup  (uvicorn start)")

    out += rect(36, 70, 148, 52, fill="#fff8e1", stroke="#c8a200")
    out += text(110, 84, "GCS  model.ubj", size=10, weight="bold")
    out += text(110, 97, "preprocessing", size=10, weight="bold")
    out += text(110, 110, "_config.json", size=10, weight="bold")
    out += text(110, 122, "feature_cols.json", size=9, fill=TEXT_SUB)

    out += rect(202, 70, 148, 52, fill=C_PROC, stroke=STROKE)
    out += text(276, 84, "Load Model", size=11, weight="bold")
    out += text(276, 98, "XGBClassifier()", size=9, fill=TEXT_SUB, family=MONO)
    out += text(276, 110, "model.load_model(tmp)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(276, 122, "json config & feature list", size=9, fill=TEXT_SUB)

    out += rect(368, 70, 180, 52, fill=C_PROC, stroke=STROKE)
    out += text(458, 84, "SHAP TreeExplainer", size=11, weight="bold")
    out += text(458, 98, "shap.TreeExplainer(model)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(458, 110, "initialized ONCE at startup", size=9, fill="#1b5e20", weight="bold")
    out += text(458, 122, "reused for all requests", size=9, fill=TEXT_SUB)

    out += rect(566, 70, 198, 52, fill=C_SVC, stroke="#0d47a1", sw=2)
    out += text(665, 84, "PredictionService", size=11, weight="bold")
    out += text(665, 98, "singleton, lazy init", size=9, fill=TEXT_SUB, family=MONO)
    out += text(665, 110, "FeatureStore(Redis)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(665, 122, "feature_refs (18 Feast refs)", size=9, fill=TEXT_SUB)

    out += rect(36, 136, 728, 44, fill="#f9fbe7", stroke=STROKE_LT)
    out += text(400, 153, "fastapi lifespan event → PredictionService.__init__()  →  startup completes in ~15-30s", size=9, fill=TEXT_GRP, family=MONO)
    out += text(400, 167, "Kafka subprocess manager (stream.py) also registers on startup", size=9, fill=TEXT_GRP)

    # ── Flow B: Prediction Request ────────────────────────────────────────
    out += group_box(20, 210, 760, 248, "Flow B — Online Prediction Request  POST /predict/")

    out += rect(36, 228, 100, 40, fill=C_EXT, stroke="#880e4f")
    out += text(86, 244, "Client", size=11, weight="bold")
    out += text(86, 256, "POST /predict/", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(154, 228, 140, 40, fill=C_SVC, stroke="#1565c0")
    out += text(224, 244, "Feast Online Fetch", size=11, weight="bold")
    out += text(224, 256, "get_online_features(msno)", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(312, 228, 100, 40, fill="#e8f5e9", stroke="#388e3c")
    out += text(362, 244, "Redis", size=11, weight="bold")
    out += text(362, 256, "10.80.68.19:6379", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(430, 228, 160, 40, fill=C_PROC, stroke=STROKE)
    out += text(510, 244, "Preprocess", size=11, weight="bold")
    out += text(510, 256, "_preprocess(features, cfg)", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(608, 228, 136, 40, fill=C_PROC, stroke=STROKE)
    out += text(676, 244, "XGBoost Infer", size=11, weight="bold")
    out += text(676, 256, "predict_proba(X)[0,1]", size=9, fill=TEXT_SUB, family=MONO)

    # Cold-start box
    out += rect(36, 290, 368, 56, fill="#fff3e0", stroke="#e65100")
    out += text(220, 305, "Cold-start: member_found = False  (user not in Redis)", size=11, weight="bold", fill="#bf360c")
    out += text(220, 320, "Zero imputation:   x_cold = 0   → churn_prob ≈ 0.71  (wrong)", size=9, fill="#c62828", family=MONO)
    out += text(220, 334, "Median imputation: x_cold = median(training)  → churn_prob ≈ 0.34  ✓", size=9, fill="#2e7d32", weight="bold", family=MONO)

    # SHAP + threshold
    out += rect(420, 290, 164, 56, fill=C_PROC, stroke=STROKE)
    out += text(502, 306, "SHAP Explanation", size=11, weight="bold")
    out += text(502, 321, "explainer.shap_values(X)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(502, 333, "top-3 positive SHAP", size=9, fill=TEXT_SUB)
    out += text(502, 345, "→ human-readable reasons", size=9, fill=TEXT_SUB)

    out += rect(600, 290, 164, 56, fill=C_PROC, stroke=STROKE)
    out += text(682, 306, "Threshold Decision", size=11, weight="bold")
    out += text(682, 321, "prob ≥ 0.781 → is_churn=1", size=9, fill=TEXT_SUB, family=MONO)
    out += text(682, 333, "env: CHURN_THRESHOLD", size=9, fill=TEXT_SUB, family=MONO)
    out += text(682, 345, "configurable, no retrain", size=9, fill=TEXT_SUB)

    # Response
    out += rect(36, 368, 728, 76, fill="#e8f4f8", stroke="#1565c0")
    out += text(400, 384, "Response  (JSON)", size=11, weight="bold")
    out += mtext(400, 396, [
        'churn_probability: 0.84  ·  is_churn: 1  ·  member_found: true',
        'reasons: ["Khách hàng đã hủy gói nhiều lần", "Ít ngày hoạt động", ...]',
        'feature_timestamp: "2017-03-05"'
    ], size=9, fill=TEXT_SUB, lh=13)

    # ── Flow C: Streaming Simulation ──────────────────────────────────────
    out += group_box(20, 476, 760, 320, "Flow C — Streaming Simulation Controller  (app/stream.py)")

    out += rect(36, 494, 140, 44, fill=C_EXT, stroke="#880e4f")
    out += text(106, 510, "Dashboard", size=11, weight="bold")
    out += text(106, 522, "POST /stream/start", size=9, fill=TEXT_SUB, family=MONO)
    out += text(106, 534, "{ speed: 55 }", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(196, 494, 176, 44, fill=C_PROC, stroke=STROKE)
    out += text(284, 510, "_kill_stale_workers()", size=10, weight="bold", family=MONO)
    out += text(284, 522, "pkill -f kafka_producer.py", size=9, fill=TEXT_SUB, family=MONO)
    out += text(284, 534, "pkill -f kafka_consumer.py", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(390, 494, 192, 44, fill=C_PROC, stroke=STROKE)
    out += text(486, 510, "① Spawn Consumer", size=11, weight="bold")
    out += text(486, 522, "offset_reset=latest", size=9, fill=TEXT_SUB, family=MONO)
    out += text(486, 534, "consumer.poll() → lock offset", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(600, 494, 164, 44, fill=C_PROC, stroke=STROKE)
    out += text(682, 510, "await sleep(20s)", size=11, weight="bold")
    out += text(682, 522, "consumer joins Kafka group", size=9, fill=TEXT_SUB)
    out += text(682, 534, "offset committed", size=9, fill=TEXT_SUB)

    out += rect(390, 556, 192, 44, fill=C_PROC, stroke=STROKE)
    out += text(486, 572, "② Spawn Producer", size=11, weight="bold")
    out += text(486, 584, "--speed 55 (s/day)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(486, 596, "GCS → Kafka replay begins", size=9, fill=TEXT_SUB)

    # SIGSTOP/SIGCONT
    out += rect(36, 556, 340, 44, fill=C_PROC, stroke=STROKE)
    out += text(206, 572, "Pause / Resume", size=11, weight="bold")
    out += text(206, 584, "SIGSTOP → producer (consumer still runs)", size=9, fill=TEXT_SUB, family=MONO)
    out += text(206, 596, "SIGCONT → resume producer", size=9, fill=TEXT_SUB, family=MONO)

    # Stop
    out += rect(600, 556, 164, 44, fill=C_PROC, stroke=STROKE)
    out += text(682, 572, "Stop", size=11, weight="bold")
    out += text(682, 584, "SIGTERM producer + consumer", size=9, fill=TEXT_SUB, family=MONO)
    out += text(682, 596, "reset all state", size=9, fill=TEXT_SUB)

    # Notify → SSE
    out += rect(36, 620, 260, 56, fill="#fff3e0", stroke="#e65100")
    out += text(166, 636, "POST /stream/notify  (from consumer)", size=11, weight="bold")
    out += text(166, 650, "date + [msno, ...]  →  update _state", size=9, fill=TEXT_SUB, family=MONO)
    out += text(166, 662, "feature_cache.update(date, msnos)", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(312, 620, 212, 56, fill=C_SVC, stroke="#1565c0")
    out += text(418, 636, "SSE Broadcast", size=11, weight="bold")
    out += text(418, 650, "event: date_done", size=9, fill=TEXT_SUB, family=MONO)
    out += text(418, 662, "data: {date, user_count, dates_done}", size=9, fill=TEXT_SUB, family=MONO)

    out += rect(540, 620, 244, 56, fill=C_EXT, stroke="#880e4f")
    out += text(662, 636, "React Dashboard /ui/", size=11, weight="bold")
    out += text(662, 650, "date chip appears  →  user list", size=9, fill=TEXT_SUB)
    out += text(662, 662, "predict button  →  POST /predict/", size=9, fill=TEXT_SUB, family=MONO)

    # Initial SSE event
    out += rect(36, 694, 748, 88, fill="#f8f8f8", stroke=STROKE_LT)
    out += text(400, 710, "GET /stream/events  —  SSE Connection", size=11, weight="bold")
    out += text(400, 726, "Initial event on connect:  event: status", size=9, fill=TEXT_SUB)
    out += text(400, 738, 'data: {"status": ..., "current_date": ..., "dates_list": [...all completed dates...]}', size=9, fill=TEXT_SUB, family=MONO)
    out += text(400, 750, "→ UI restores full state on reconnect / tab switch  ·  keepalive comment every 20s", size=9, fill=TEXT_GRP)
    out += text(400, 764, "All 6 pages mounted simultaneously with CSS display:none/block  →  SSE connection preserved across tab switches", size=9, fill="#1b5e20", weight="bold")

    # ── Arrows flow A ─────────────────────────────────────────────────────
    out += arrow(110, 70, 202, 96)
    out += arrow(276, 122, 368, 96)
    out += arrow(566, 96, 458, 122)

    # ── Arrows flow B ─────────────────────────────────────────────────────
    out += arrow(136, 248, 154, 248)
    out += arrow(294, 248, 312, 248)
    out += arrow(412, 248, 430, 248)
    out += arrow(590, 248, 608, 248)
    out += arrow(676, 268, 676, 290)
    out += arrow(510, 268, 502, 290)
    out += arrow(600, 318, 502, 318, dash=True)

    # Cold-start → preprocess
    out += path_arrow(f"M 220 268 L 220 290", dash=False)

    # ── Arrows flow C ─────────────────────────────────────────────────────
    out += arrow(176, 516, 196, 516)
    out += arrow(372, 516, 390, 516)
    out += arrow(682, 538, 682, 556)
    out += arrow(600, 556, 582, 578, dash=True)
    out += arrow(166, 600, 166, 620)
    out += arrow(296, 648, 312, 648)
    out += arrow(524, 648, 540, 648)
    out += arrow(486, 600, 486, 620, dash=True)

    out += svg_close()
    return out


# ══════════════════════════════════════════════════════════════════════════════
# DIAGRAM 5 — Monitoring Pipeline (detailed)
# ══════════════════════════════════════════════════════════════════════════════
def diag_monitoring():
    W, H = 760, 660
    out = svg_open(W, H) + ARROW_DEF

    out += text(W//2, 26, "Monitoring Pipeline — Detailed Architecture",
                size=14, weight="bold")
    out += text(W//2, 42, "Prometheus Scraping · Grafana Dashboards · Business React Dashboard",
                size=10, fill=TEXT_SUB)

    # ── Instrumentation ───────────────────────────────────────────────────
    out += group_box(20, 52, 720, 152, "Instrumentation  (serving_pipeline/app/metrics.py)")

    out += rect(36, 70, 690, 32, fill=C_SVC, stroke="#1565c0", sw=2)
    out += text(381, 83, "FastAPI Middleware  —  auto-instrument all HTTP endpoints  →  GET /metrics  (Prometheus text format)", size=10, weight="bold")
    out += text(381, 95, "9 metric definitions using prometheus-client library  ·  scrape interval: 15s", size=9, fill=TEXT_SUB)

    # 9 metrics table
    metrics = [
        ("serving_http_requests_total", "Counter", "method, path, status_code", "Total HTTP requests"),
        ("serving_http_request_duration_seconds", "Histogram", "method, path", "Request latency  (buckets: 5ms–10s)"),
        ("serving_http_requests_in_progress", "Gauge", "method, path", "In-flight requests"),
        ("serving_prediction_requests_total", "Counter", "endpoint, kind", "single / batch calls"),
        ("serving_prediction_results_total", "Counter", "endpoint, is_churn", "Churn vs retain counts"),
        ("serving_prediction_churn_probability", "Histogram", "endpoint", "Score distribution (0.0–1.0)"),
        ("serving_batch_prediction_size", "Histogram", "—", "Records per batch call"),
        ("serving_feast_online_fetch_total", "Counter", "status (ok/error)", "Feast Redis lookups"),
        ("serving_feast_online_fetch_duration_seconds", "Histogram", "status", "Feast lookup latency"),
    ]
    col_x = [36, 296, 396, 522]
    hdrs = ["Metric Name", "Type", "Labels", "Description"]
    row_h = 14
    row0 = 116
    # header
    for i, h in enumerate(hdrs):
        out += text(col_x[i] + 4, row0, h, size=9, weight="bold", fill=TEXT_MAIN, anchor="start")
    out += hr(36, 726, row0 + 4, dash=False, stroke="#bbb")
    for r, (name, typ, lbl, desc) in enumerate(metrics):
        y = row0 + 8 + (r+1) * row_h
        bg = "#f9f9f9" if r % 2 == 0 else "#ffffff"
        out += f'<rect x="36" y="{y-10}" width="690" height="{row_h}" fill="{bg}"/>\n'
        out += text(col_x[0]+4, y, name, size=8, fill="#1565c0", anchor="start", family=MONO)
        out += text(col_x[1]+4, y, typ, size=8, fill=TEXT_MAIN, anchor="start")
        out += text(col_x[2]+4, y, lbl, size=8, fill=TEXT_SUB, anchor="start")
        out += text(col_x[3]+4, y, desc, size=8, fill=TEXT_SUB, anchor="start")

    # ── Prometheus ────────────────────────────────────────────────────────
    out += group_box(20, 222, 340, 120, "Prometheus  :9090  (Docker)")
    out += rect(36, 240, 308, 44, fill="#fff3e0", stroke="#e65100")
    out += text(190, 256, "prometheus.yml scrape config", size=11, weight="bold")
    out += text(190, 269, "targets: [localhost:8000]", size=9, fill=TEXT_SUB, family=MONO)
    out += text(190, 281, "scrape_interval: 15s  ·  job: kkbox_serving_api", size=9, fill=TEXT_SUB, family=MONO)
    out += rect(36, 294, 308, 40, fill=C_PROC, stroke=STROKE)
    out += text(190, 310, "PromQL  (example queries)", size=11, weight="bold")
    out += text(190, 323, "rate(http_requests_total[1m])", size=9, fill=TEXT_SUB, family=MONO)
    out += text(190, 333, "histogram_quantile(0.99, rate(duration_bucket[5m]))", size=8, fill=TEXT_SUB, family=MONO)

    # ── Grafana ───────────────────────────────────────────────────────────
    out += group_box(380, 222, 360, 120, "Grafana  :3000  (Docker)")
    out += rect(396, 240, 328, 44, fill="#fff3e0", stroke="#e65100")
    out += text(560, 256, "Auto-provisioned datasource", size=11, weight="bold")
    out += text(560, 269, "grafana/provisioning/datasources/", size=9, fill=TEXT_SUB, family=MONO)
    out += text(560, 281, "prometheus.yml  →  no manual setup", size=9, fill=TEXT_SUB, family=MONO)
    out += rect(396, 294, 328, 40, fill=C_PROC, stroke=STROKE)
    out += text(560, 310, "Dashboard JSON auto-load", size=11, weight="bold")
    out += text(560, 323, "grafana/dashboards/*.json", size=9, fill=TEXT_SUB, family=MONO)
    out += text(560, 333, "commit to repo for persistence", size=9, fill=TEXT_SUB)

    # ── Dashboard 1 ───────────────────────────────────────────────────────
    out += group_box(20, 362, 340, 180, "Grafana Dashboard 1 — API Health")
    panels1 = [
        ("Request Rate (RPS)",   "rate(serving_http_requests_total[1m])"),
        ("P99 Latency",          "histogram_quantile(0.99, rate(..._duration_bucket[5m]))"),
        ("Error Rate",           "rate(...{status_code=~'5..'}[1m]) / rate(...[1m])"),
        ("In-Flight Requests",   "serving_http_requests_in_progress"),
    ]
    for i, (pname, pql) in enumerate(panels1):
        y = 380 + i * 38
        out += rect(36, y, 308, 32, fill=C_PROC, stroke=STROKE_LT)
        out += text(44, y + 12, pname, size=10, weight="bold", anchor="start")
        out += text(44, y + 24, pql, size=8, fill=TEXT_SUB, anchor="start", family=MONO)

    # ── Dashboard 2 ───────────────────────────────────────────────────────
    out += group_box(380, 362, 360, 180, "Grafana Dashboard 2 — Churn Prediction")
    panels2 = [
        ("Churn Rate (rolling 5m)",       "rate(...is_churn='True'[5m]) / rate(...[5m])"),
        ("Prediction Throughput",          "rate(serving_prediction_requests_total[1m])"),
        ("Churn Probability Distribution", "rate(churn_probability_bucket[5m])  [heatmap]"),
        ("Feast P95 Fetch Latency",        "histogram_quantile(0.95, rate(...fetch_duration[5m]))"),
    ]
    for i, (pname, pql) in enumerate(panels2):
        y = 380 + i * 38
        out += rect(396, y, 328, 32, fill=C_PROC, stroke=STROKE_LT)
        out += text(404, y + 12, pname, size=10, weight="bold", anchor="start")
        out += text(404, y + 24, pql, size=8, fill=TEXT_SUB, anchor="start", family=MONO)

    # ── Business Dashboard ────────────────────────────────────────────────
    out += group_box(20, 560, 720, 84, "React Business Dashboard  /ui/  (served by FastAPI StaticFiles, no build step)")
    pages = [
        ("Single User", "Predict + SHAP reasons"),
        ("Batch Predict", "List msno → table"),
        ("Statistics", "Cumulative stats"),
        ("Streaming Sim", "Date chips + SSE"),
        ("Model Info", "Metrics + features"),
        ("API Health", "Endpoint latency"),
    ]
    for i, (p, d) in enumerate(pages):
        x = 36 + i * 118
        out += rect(x, 576, 112, 56, fill=C_EXT, stroke="#880e4f")
        out += text(x+56, 594, p, size=10, weight="bold")
        out += text(x+56, 607, d, size=8, fill=TEXT_SUB)
        out += text(x+56, 619, "CSS display:", size=8, fill=TEXT_GRP)
        out += text(x+56, 629, "none / block", size=8, fill=TEXT_GRP)

    # ── Arrows ────────────────────────────────────────────────────────────
    out += arrow(190, 204, 190, 240)
    out += arrow(560, 204, 560, 240)
    out += arrow(360, 312, 396, 312)

    out += svg_close()
    return out


# ── Write all files ────────────────────────────────────────────────────────
diagrams = {
    "fig1_overall.svg":    diag_overall,
    "fig2_data.svg":       diag_data,
    "fig3_model.svg":      diag_model,
    "fig4_serving.svg":    diag_serving,
    "fig5_monitoring.svg": diag_monitoring,
}
for fname, fn in diagrams.items():
    path = os.path.join(OUT, fname)
    with open(path, "w") as f:
        f.write(fn())
    print(f"Written: {path}")
