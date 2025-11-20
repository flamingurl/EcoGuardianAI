# Problem 1 Solution: Anomoly Detection
import uuid, math
from datetime import datetime, timedelta
from collections import deque

W_SECONDS = 30*60   # rolling window length (2 minute example) - can change
Z_THRESHOLD = 2.5
EPS = 1e-9

# in-memory per-room per-metric deque of (ts, value)
buffers = {}  # key: (roomId, metric) -> deque of (ts, value)

def push_sample(roomId, metric, ts, value):
    key = (roomId, metric)
    buf = buffers.setdefault(key, deque())
    buf.append((ts, value))
    # purge old
    cutoff = ts - timedelta(seconds=W_SECONDS)
    while buf and buf[0][0] < cutoff:
        buf.popleft()
    return check_anomaly(roomId, metric)

def check_anomaly(roomId, metric):
    buf = buffers.get((roomId, metric))
    if not buf or len(buf) < 3:
        return None
    vals = [v for (_, v) in buf]
    n = len(vals)
    mu = sum(vals)/n
    var = sum((v-mu)**2 for v in vals)/n
    sigma = math.sqrt(var)
    latest = vals[-1]
    z = (latest - mu) / (sigma + EPS)
    if abs(z) > Z_THRESHOLD:
        anomaly = {
            "id": f"anom-{roomId}-{metric}-{int(datetime.utcnow().timestamp())}",
            "roomId": roomId,
            "tsStart": buf[0][0].isoformat(),
            "tsEnd": buf[-1][0].isoformat(),
            "metric": metric,
            "value": float(latest),
            "z": float(z),
            "reason": "rolling-z"
        }
        write_anomaly(anomaly)  # implement Cosmos upsert
        return anomaly
    return None
