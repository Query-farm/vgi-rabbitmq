"""Generate handwritten.json: responses a healthy single-node fixture broker
cannot produce on demand. All live under the /broken path prefix, so a secret
with MANAGEMENT_URL '<fixture>/broken' sees an unhealthy, older broker.

    python3 test/fixture_server/make_handwritten.py
"""

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
P = "GET /broken/api"


def ok(body, status=200):
    return {"status": status, "body": json.dumps(body)}


F = {
    P + "/health/checks/alarms": ok({"status": "failed", "reason": "There are alarms in effect in the cluster",
                                     "alarms": [{"node": "rabbit@b", "resource": "memory"}]}, 503),
    P + "/health/checks/local-alarms": ok({"status": "failed", "reason": "There are alarms in effect on the node",
                                           "alarms": [{"node": "rabbit@b", "resource": "disk"}]}, 503),
    P + "/health/checks/certificate-expiration/30/days": ok({"status": "failed", "reason": "Certificates expiring",
                                                            "expired": [{"node": "rabbit@a", "protocol": "amqp/ssl", "port": 5671,
                                                                         "certfile": "/etc/rabbitmq/server.pem"}]}, 503),
    P + "/health/checks/node-is-quorum-critical": ok({"status": "failed", "reason": "There are quorum queues that would lose their quorum if the target node is shut down",
                                                      "queues": [{"name": "payments", "virtual_host": "shop"}]}, 503),
    P + "/health/checks/virtual-hosts": ok({"status": "ok"}),
    # Older brokers do not have these endpoints: the rows are omitted.
    P + "/health/checks/is-in-service": ok({"error": "Object Not Found", "reason": "Not Found"}, 404),
    P + "/health/checks/below-node-connection-limit": ok({"error": "Object Not Found", "reason": "Not Found"}, 404),
    P + "/health/checks/ready-to-serve-clients": ok({"error": "Object Not Found", "reason": "Not Found"}, 404),
    P + "/overview": ok({"error": "internal_error", "reason": "management database unavailable"}, 500),
    P + "/nodes": ok([
        {"name": "rabbit@a", "type": "disc", "running": True, "uptime": 86400000, "mem_used": 900, "mem_limit": 1000,
         "mem_alarm": False, "disk_free": 10 ** 10, "disk_free_limit": 5 * 10 ** 7, "disk_free_alarm": False, "fd_used": 10,
         "fd_total": 1024, "partitions": ["rabbit@b"], "being_drained": False, "rabbitmq_version": "3.13.7",
         "enabled_plugins": ["rabbitmq_management"]},
        {"name": "rabbit@b", "type": "disc", "running": True, "uptime": 3600000, "mem_used": 1200, "mem_limit": 1000,
         "mem_alarm": True, "disk_free": 10 ** 7, "disk_free_limit": 5 * 10 ** 7, "disk_free_alarm": True, "fd_used": 1000,
         "fd_total": 1024, "partitions": ["rabbit@a"], "being_drained": True, "rabbitmq_version": "3.13.7",
         "enabled_plugins": ["rabbitmq_management"]},
        {"name": "rabbit@c", "running": False, "partitions": []},
    ]),
    # 3.x answers /api/users with password hashes; the worker must never return them.
    P + "/users": ok([{"name": "legacy", "password_hash": "c2VjcmV0aGFzaA==", "hashing_algorithm": "rabbit_password_hashing_sha256",
                       "tags": "administrator,monitoring", "limits": {}},
                      {"name": "nopass", "password_hash": "", "tags": "", "limits": {}}]),
    P + "/parameters": ok([
        {"vhost": "/", "component": "shovel", "name": "multi",
         "value": {"src-uri": ["amqp://a:pw1@h1", "amqp://b:pw2@h2/%2f?heartbeat=10&password=pw3"],
                   "dest-uri": "amqps://c:p%40ss@h3:5671", "dest-password": "pw4", "src-queue": "q"}},
    ]),
    P + "/queues?page=1&page_size=100": ok({"error": "bad_request", "reason": "Page out of range"}, 400),
}

with open(os.path.join(HERE, "handwritten.json"), "w") as f:
    json.dump(F, f, indent=1, sort_keys=True)
    f.write("\n")
print(len(F), "handwritten responses")
