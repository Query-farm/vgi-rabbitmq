"""Publish the fixture's deterministic messages through the management API.

    python3 test/fixture/seed.py http://127.0.0.1:35672

Every message has a fixed payload, routing key, headers and AMQP properties
(timestamps included), so peeks return the same rows on every run. The
`orders` topic exchange fans each order out to orders.created / orders.archive
and the `events` stream; orders.paid also reaches the `payments` quorum queue.
"""

import base64
import json
import sys
import time
import urllib.request

BASE = sys.argv[1].rstrip("/")
AUTH = "Basic " + base64.b64encode(b"admin:admin-pass").decode()


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": AUTH, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read() or b"null")


def publish(exchange, key, payload, props, encoding="string"):
    r = call("POST", f"/api/exchanges/shop/{exchange or 'amq.default'}/publish",
             {"properties": props, "routing_key": key, "payload": payload, "payload_encoding": encoding})
    assert r["routed"], (exchange, key)


T0 = 1767225600  # 2026-01-01 00:00:00 UTC
ORDERS = [
    ("orders.created.eu", {"order_id": 1001, "region": "eu", "total": 42.5}),
    ("orders.created.us", {"order_id": 1002, "region": "us", "total": 19.99}),
    ("orders.created.eu", {"order_id": 1003, "region": "eu", "total": 7.25}),
    ("orders.paid", {"order_id": 1001, "method": "card"}),
    ("orders.paid", {"order_id": 1002, "method": "paypal"}),
]
for i, (key, body) in enumerate(ORDERS):
    publish("orders", key, json.dumps(body, sort_keys=True), {
        "content_type": "application/json",
        "delivery_mode": 2,
        "message_id": f"msg-{i + 1}",
        "correlation_id": f"order-{body['order_id']}",
        "app_id": "shop-api",
        "type": key,
        "timestamp": T0 + 60 * i,
        "headers": {"tenant": "acme", "attempt": i % 2 + 1, "trace": f"t-{i + 1:03d}"},
    })
# A binary (non-UTF-8) payload straight to a queue through the default exchange.
publish(None, "orders.dlq", base64.b64encode(bytes([0, 255, 254, 1])).decode(),
        {"content_type": "application/octet-stream", "message_id": "bin-1", "timestamp": T0 + 600}, "base64")
# The stream must hold every order: fail loudly rather than record a wrong fixture.
for _ in range(30):
    if call("GET", "/api/queues/shop/events").get("messages") == len(ORDERS):
        break
    time.sleep(1)
else:
    sys.exit("the events stream did not receive the seeded messages")
print("seeded", len(ORDERS) + 1, "messages")
