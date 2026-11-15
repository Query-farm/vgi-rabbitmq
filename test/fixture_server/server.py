"""Replay (or record) RabbitMQ Management API responses for the SQL tests.

    python3 test/fixture_server/server.py                       # replay
    python3 test/fixture_server/server.py --record http://127.0.0.1:35672

Prints ``PORT:<n>`` on stdout once listening (ci/run-integration.sh waits for
it). Responses are keyed by method, raw path (vhost "/" stays %2F), sorted
query parameters and, for POST, the sorted JSON body. ``recorded.json`` holds
responses captured from the live fixture broker (test/fixture/run-rabbitmq.sh);
``handwritten.json`` holds shapes a healthy single-node broker will not
produce on demand (failing health checks, alarms, partitions, errors), served
under the /broken path prefix.

Environment:
  FIXTURE_FLAKY=1          answer every key with 429 + Retry-After: 0 once,
                           and 503 the second time, so retries are exercised
  FIXTURE_REQUIRE_AUTH=1   demand "Bearer test-token" or basic auth
                           monitor:monitor-pass (401 otherwise)
  FIXTURE_UPSTREAM_AUTH    user:password used when recording (default admin:admin-pass)
"""

import base64
import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
RECORDED = os.path.join(HERE, "recorded.json")
HANDWRITTEN = os.path.join(HERE, "handwritten.json")

UPSTREAM = None
if "--record" in sys.argv:
    UPSTREAM = sys.argv[sys.argv.index("--record") + 1].rstrip("/")
UPSTREAM_AUTH = "Basic " + base64.b64encode(os.environ.get("FIXTURE_UPSTREAM_AUTH", "admin:admin-pass").encode()).decode()

LOCK = threading.Lock()


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


FIXTURES = load(RECORDED)
FIXTURES.update(load(HANDWRITTEN))
SEEN = {}
GOOD_AUTH = {"Bearer test-token", "Basic " + base64.b64encode(b"monitor:monitor-pass").decode()}


def key_of(method, path, query, body):
    items = sorted(urllib.parse.parse_qsl(query, keep_blank_values=True))
    k = method + " " + path + ("?" + urllib.parse.urlencode(items) if items else "")
    if body:
        k += " " + json.dumps(json.loads(body), sort_keys=True)
    return k


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, status, body, headers=None):
        data = body if isinstance(body, bytes) else body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def handle_any(self, method, body=b""):
        url = urllib.parse.urlsplit(self.path)
        if os.environ.get("FIXTURE_REQUIRE_AUTH") == "1" and self.headers.get("Authorization") not in GOOD_AUTH:
            return self.reply(401, '{"error":"not_authorized","reason":"Login failed"}')
        key = key_of(method, url.path, url.query, body)
        if os.environ.get("FIXTURE_FLAKY") == "1":
            with LOCK:
                n = SEEN.get(key, 0)
                SEEN[key] = n + 1
            if n == 0:
                return self.reply(429, '{"error":"too_many_requests","reason":"slow down"}', {"Retry-After": "0"})
            if n == 1:
                return self.reply(503, '{"error":"unavailable","reason":"try again"}', {"Retry-After": "0"})
        if key in FIXTURES:
            entry = FIXTURES[key]
            return self.reply(entry["status"], entry["body"], entry.get("headers"))
        if UPSTREAM is None:
            return self.reply(404, json.dumps({"error": "not_recorded", "reason": "no recorded response for " + key}))
        req = urllib.request.Request(UPSTREAM + self.path, method=method, data=body or None,
                                     headers={"Authorization": UPSTREAM_AUTH, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                status, data = resp.status, resp.read()
        except urllib.error.HTTPError as e:
            status, data = e.code, e.read()
        with LOCK:
            FIXTURES[key] = {"status": status, "body": data.decode()}
            recorded = load(RECORDED)
            recorded[key] = FIXTURES[key]
            with open(RECORDED, "w") as f:
                json.dump(recorded, f, indent=0, sort_keys=True)
                f.write("\n")
        self.reply(status, data)

    def do_GET(self):
        self.handle_any("GET")

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        self.handle_any("POST", self.rfile.read(n))


def main():
    srv = ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("FIXTURE_PORT", "0"))), Handler)
    print(f"PORT:{srv.server_address[1]}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
