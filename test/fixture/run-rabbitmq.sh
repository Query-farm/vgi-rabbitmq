#!/usr/bin/env bash
# Start the live fixture: rabbitmq:4-management with deterministic definitions.
#
#   test/fixture/run-rabbitmq.sh start   # prints the management URL and AMQP URL
#   test/fixture/run-rabbitmq.sh stop
#
# definitions.json is imported at boot (vhosts, users, permissions, a classic,
# a quorum and a stream queue, exchanges, bindings, a policy, a shovel and a
# federation upstream with fake credentials in their URIs); seed.py then
# publishes fixed messages. The node name is fixed (rabbit@vgi-rabbit) so
# recorded responses do not depend on the container id. Memory is capped at 512 MB.
set -euo pipefail
IMAGE="${RABBITMQ_IMAGE:-rabbitmq:4-management}"
NAME="${RABBITMQ_CONTAINER:-vgi-rabbitmq-fixture}"
HTTP_PORT="${RABBITMQ_HTTP_PORT:-35672}"
AMQP_PORT="${RABBITMQ_AMQP_PORT:-35673}"
HERE="$(cd "$(dirname "$0")" && pwd)"
case "${1:-start}" in
  stop) docker rm -f "$NAME" >/dev/null 2>&1 || true; exit 0 ;;
  start) ;;
  *) echo "usage: $0 start|stop"; exit 2 ;;
esac
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" --hostname vgi-rabbit -m 512m \
  -p "127.0.0.1:$HTTP_PORT:15672" -p "127.0.0.1:$AMQP_PORT:5672" \
  -v "$HERE/rabbitmq.conf:/etc/rabbitmq/conf.d/90-fixture.conf:ro" \
  -v "$HERE/definitions.json:/etc/rabbitmq/definitions.json:ro" \
  -v "$HERE/enabled_plugins:/etc/rabbitmq/enabled_plugins:ro" \
  "$IMAGE" >/dev/null
for _ in $(seq 1 90); do
  # Seed only once the stream is running and bound, or its copies are lost.
  if curl -fsS -u admin:admin-pass "http://127.0.0.1:$HTTP_PORT/api/health/checks/virtual-hosts" >/dev/null 2>&1 \
     && curl -fsS -u admin:admin-pass "http://127.0.0.1:$HTTP_PORT/api/queues/shop/events" 2>/dev/null | grep -q '"state":"running"' \
     && curl -fsS -u admin:admin-pass "http://127.0.0.1:$HTTP_PORT/api/bindings/shop/e/orders/q/events" 2>/dev/null | grep -q '"routing_key"'; then
    sleep 3
    python3 "$HERE/seed.py" "http://127.0.0.1:$HTTP_PORT" >&2
    sleep 6   # let the management stats catch up with the published messages
    echo "http://127.0.0.1:$HTTP_PORT"
    echo "amqp://admin:admin-pass@127.0.0.1:$AMQP_PORT/shop"
    exit 0
  fi
  sleep 1
done
docker logs "$NAME" >&2; exit 1
