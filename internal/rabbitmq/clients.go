// Client functions: connections, channels and consumers.

package rabbitmq

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// clientScoped is /api/<base> or /api/vhosts/{vhost}/<base>.
func clientScoped(base string, vhost *string) string {
	if vhost == nil {
		return "/" + base
	}
	return "/vhosts/" + seg(*vhost) + "/" + base
}

// ---------------------------------------------------------------- connections

var connectionsColumns = []arrow.Field{
	column("vhost", str, "Virtual host the connection is open on.", false),
	column("name", str, "Connection name, e.g. '172.17.0.1:50412 -> 172.17.0.2:5672'.", false),
	column("user", str, "Authenticated user.", true),
	column("node", str, "Node holding the connection.", true),
	column("state", str, "running, blocking, blocked (by a resource alarm), flow or closing.", true),
	column("protocol", str, "Protocol, e.g. 'AMQP 0-9-1', 'AMQP 1.0', 'MQTT 5-0', 'Stream'.", true),
	column("auth_mechanism", str, "SASL mechanism, e.g. PLAIN or EXTERNAL.", true),
	column("ssl", boolean, "Whether the connection uses TLS.", true),
	column("peer_host", str, "Client address.", true),
	column("peer_port", i64, "Client port.", true),
	column("channels", i64, "Open channels on the connection.", true),
	column("connected_at", tstz, "When the connection opened.", true),
	column("client_connection_name", str, "Name the client gave the connection (client_properties.connection_name); NULL when unset.", true),
	column("client_product", str, "Client library product name, e.g. 'RabbitMQ' or 'pika'.", true),
	column("client_version", str, "Client library version.", true),
	column("client_platform", str, "Client platform, e.g. 'Go', 'Python 3.12'.", true),
	column("heartbeat_seconds", i64, "Negotiated heartbeat timeout in seconds (0 = off).", true),
	column("recv_oct", i64, "Bytes received.", true),
	column("send_oct", i64, "Bytes sent.", true),
	column("recv_rate", f64, "Bytes received per second.", true),
	column("send_rate", f64, "Bytes sent per second.", true),
}

var connectionsExamples = []vgi.CatalogExample{
	{SQL: "SELECT user, client_product, count(*) AS connections FROM rabbitmq.main.connections() GROUP BY ALL ORDER BY connections DESC, user",
		Description: "Connections per user and client library"},
	{SQL: "SELECT name, user, state FROM rabbitmq.main.connections(vhost := 'shop') WHERE state IN ('blocked', 'blocking') ORDER BY name",
		Description: "Connections blocked by a resource alarm in one vhost"},
}

func newConnections() vgi.TableFunction {
	return newList(&listFunc[pagedArgs]{
		name: "connections", columns: connectionsColumns,
		meta: meta(docs{Title: "Client Connections", Category: "clients",
			Keywords: "connections, clients, blocked publishers, tls, client library, /api/connections",
			DocLLM: "One row per client connection from GET /api/connections or /api/vhosts/{vhost}/connections (paginated): vhost, name, " +
				"user, node, state (blocked = held by a resource alarm), protocol, auth_mechanism, ssl, peer_host/peer_port, channels, " +
				"connected_at, client_connection_name/product/version/platform, heartbeat_seconds, bytes and byte rates. " + pagedDoc,
			DocMD: "## connections(vhost := NULL, name := NULL, use_regex := false, page_size := 100)\n\nEvery client connection with user, client library, state and traffic."},
			"Client connections with user, client library, TLS, state and traffic", connectionsColumns, connectionsExamples),
		fetch: func(ctx context.Context, c *conn, a pagedArgs, pg int64) ([][]any, bool, error) {
			return fetchPaged(ctx, c, clientScoped("connections", a.Vhost), a, pg, func(m map[string]any) []any {
				return []any{vStr(m["vhost"]), vStr(m["name"]), vStrNE(m["user"]), vStrNE(m["node"]), vStrNE(m["state"]),
					vStrNE(m["protocol"]), vStrNE(m["auth_mechanism"]), vBool(m["ssl"]), vStrNE(m["peer_host"]), vInt(m["peer_port"]),
					vInt(m["channels"]), vEpochMs(m["connected_at"]),
					vStrNE(at(m, "client_properties", "connection_name")), vStrNE(at(m, "client_properties", "product")),
					vStrNE(at(m, "client_properties", "version")), vStrNE(at(m, "client_properties", "platform")),
					vInt(m["timeout"]), vInt(m["recv_oct"]), vInt(m["send_oct"]), rate(m, "", "recv_oct"), rate(m, "", "send_oct")}
			})
		},
	})
}

// ---------------------------------------------------------------- channels

var channelsColumns = []arrow.Field{
	column("vhost", str, "Virtual host.", false),
	column("name", str, "Channel name: '<connection name> (<number>)'.", false),
	column("connection_name", str, "Connection the channel belongs to.", true),
	column("number", i64, "Channel number within its connection.", true),
	column("user", str, "Authenticated user.", true),
	column("node", str, "Node holding the channel.", true),
	column("state", str, "running, flow, starting or closing.", true),
	column("consumer_count", i64, "Consumers on the channel.", true),
	column("prefetch_count", i64, "Per-consumer prefetch (basic.qos); 0 = unlimited.", true),
	column("global_prefetch_count", i64, "Per-channel prefetch; 0 = unlimited.", true),
	column("messages_unacknowledged", i64, "Messages delivered on this channel and not yet acknowledged.", true),
	column("messages_unconfirmed", i64, "Published messages awaiting a publisher confirm.", true),
	column("messages_uncommitted", i64, "Messages in an open transaction.", true),
	column("confirm", boolean, "Publisher confirms enabled.", true),
	column("transactional", boolean, "Channel is in transaction mode.", true),
	column("publish_rate", f64, "Messages published per second.", true),
	column("deliver_get_rate", f64, "Messages delivered per second.", true),
	column("ack_rate", f64, "Acknowledgements per second.", true),
	column("idle_since", tstz, "When the channel went idle; NULL when active.", true),
}

var channelsExamples = []vgi.CatalogExample{
	{SQL: "SELECT name, user, prefetch_count, messages_unacknowledged FROM rabbitmq.main.channels() WHERE messages_unacknowledged > 0 ORDER BY messages_unacknowledged DESC, name",
		Description: "Channels holding unacknowledged messages"},
	{SQL: "SELECT vhost, count(*) AS channels, sum(consumer_count) AS consumers FROM rabbitmq.main.channels() GROUP BY vhost ORDER BY vhost",
		Description: "Channels and consumers per vhost"},
}

func newChannels() vgi.TableFunction {
	return newList(&listFunc[pagedArgs]{
		name: "channels", columns: channelsColumns,
		meta: meta(docs{Title: "Channels", Category: "clients",
			Keywords: "channels, prefetch, unacknowledged, publisher confirms, qos, /api/channels",
			DocLLM: "One row per channel from GET /api/channels or /api/vhosts/{vhost}/channels (paginated): vhost, name, connection_name, " +
				"number, user, node, state, consumer_count, prefetch_count, global_prefetch_count, messages_unacknowledged/unconfirmed/uncommitted, " +
				"confirm, transactional, publish/deliver_get/ack rates, idle_since. " + pagedDoc,
			DocMD: "## channels(vhost := NULL, name := NULL, use_regex := false, page_size := 100)\n\nEvery channel with prefetch, unacknowledged messages and rates."},
			"Channels with prefetch, unacknowledged and unconfirmed messages and rates", channelsColumns, channelsExamples),
		fetch: func(ctx context.Context, c *conn, a pagedArgs, pg int64) ([][]any, bool, error) {
			return fetchPaged(ctx, c, clientScoped("channels", a.Vhost), a, pg, func(m map[string]any) []any {
				return []any{vStr(m["vhost"]), vStr(m["name"]), vStrNE(at(m, "connection_details", "name")), vInt(m["number"]),
					vStrNE(m["user"]), vStrNE(m["node"]), vStrNE(m["state"]), vInt(m["consumer_count"]), vInt(m["prefetch_count"]),
					vInt(m["global_prefetch_count"]), vInt(m["messages_unacknowledged"]), vInt(m["messages_unconfirmed"]),
					vInt(m["messages_uncommitted"]), vBool(m["confirm"]), vBool(m["transactional"]),
					rate(m, "message_stats", "publish"), rate(m, "message_stats", "deliver_get"), rate(m, "message_stats", "ack"),
					vTime(m["idle_since"])}
			})
		},
	})
}

// ---------------------------------------------------------------- consumers

var consumersColumns = []arrow.Field{
	column("vhost", str, "Virtual host of the queue.", false),
	column("queue", str, "Queue being consumed.", false),
	column("consumer_tag", str, "Consumer tag.", false),
	column("channel_name", str, "Channel the consumer is on.", true),
	column("connection_name", str, "Connection the consumer is on.", true),
	column("peer_host", str, "Client address.", true),
	column("user", str, "User the consumer authenticated as.", true),
	column("ack_required", boolean, "Manual acknowledgement mode (false = auto-ack).", true),
	column("prefetch_count", i64, "Prefetch limit for this consumer; 0 = unlimited.", true),
	column("exclusive", boolean, "Exclusive consumer.", true),
	column("active", boolean, "Whether the consumer receives deliveries (false for a waiting single-active consumer).", true),
	column("activity_status", str, "up, waiting or single_active.", true),
	column("arguments", strMap, "Consumer arguments, e.g. x-stream-offset or x-priority (values as text).", false),
}

var consumersExamples = []vgi.CatalogExample{
	{SQL: "SELECT vhost, queue, count(*) AS consumers, sum(prefetch_count) AS total_prefetch FROM rabbitmq.main.consumers() GROUP BY vhost, queue ORDER BY consumers DESC, vhost, queue",
		Description: "Consumers and combined prefetch per queue"},
	{SQL: "SELECT q.name, q.messages_ready, c.consumer_tag, c.prefetch_count FROM rabbitmq.main.queues(vhost := 'shop') q LEFT JOIN rabbitmq.main.consumers(vhost := 'shop') c ON c.queue = q.name ORDER BY q.name, c.consumer_tag",
		Description: "Each queue's backlog next to who consumes it"},
}

func newConsumers() vgi.TableFunction {
	return newList(&listFunc[vhostArgs]{
		name: "consumers", columns: consumersColumns,
		meta: meta(docs{Title: "Consumers", Category: "clients",
			Keywords: "consumers, subscriptions, prefetch, single active consumer, ack mode, /api/consumers",
			DocLLM: "One row per consumer from GET /api/consumers[/{vhost}]: vhost, queue, consumer_tag, channel_name, connection_name, " +
				"peer_host, user, ack_required, prefetch_count, exclusive, active, activity_status, arguments `MAP`. " +
				"Named args: vhost (pushed into the path), secret_name. Join with queues on (vhost, queue) to compare backlog and consumers.",
			DocMD: "## consumers(vhost := NULL)\n\nEvery consumer with its queue, channel, ack mode and prefetch."},
			"Consumers with their queue, channel, ack mode, prefetch and activity", consumersColumns, consumersExamples),
		fetch: func(ctx context.Context, c *conn, a vhostArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, vhostPath("/consumers", a.Vhost), nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				return []any{vStr(at(m, "queue", "vhost")), vStr(at(m, "queue", "name")), vStr(m["consumer_tag"]),
					vStrNE(at(m, "channel_details", "name")), vStrNE(at(m, "channel_details", "connection_name")),
					vStrNE(at(m, "channel_details", "peer_host")), vStrNE(at(m, "channel_details", "user")),
					vBool(m["ack_required"]), vInt(m["prefetch_count"]), vBool(m["exclusive"]), vBool(m["active"]),
					vStrNE(m["activity_status"]), vMap(m["arguments"])}
			}), false, nil
		},
	})
}
