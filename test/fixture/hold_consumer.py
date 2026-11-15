"""Hold a consumer open on shop/orders.archive so connections, channels and
consumers are non-empty while recording. It never acknowledges, so with a
prefetch of 2 the queue shows 2 unacknowledged and 1 ready message.

    uv run --no-project --with pika==1.3.2 python test/fixture/hold_consumer.py amqp://admin:admin-pass@127.0.0.1:35673/shop
"""

import sys
import time

import pika

params = pika.URLParameters(sys.argv[1])
params.client_properties = {"connection_name": "fixture-archiver", "product": "fixture-archiver"}
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.basic_qos(prefetch_count=2)
ch.basic_consume("orders.archive", lambda *a: None, auto_ack=False, consumer_tag="archiver-1")
print("consuming", flush=True)
while True:
    conn.process_data_events(time_limit=1)
    time.sleep(0.2)
