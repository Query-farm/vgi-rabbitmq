// Broker-level functions: overview, nodes, vhosts, health_checks,
// feature_flags, deprecated_features and definitions_summary.

package rabbitmq

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// ---------------------------------------------------------------- overview

var overviewColumns = []arrow.Field{
	column("cluster_name", str, "Cluster name (set with rabbitmqctl set_cluster_name).", true),
	column("node", str, "The node that answered the request, e.g. 'rabbit@host'.", true),
	column("rabbitmq_version", str, "RabbitMQ server version, e.g. '4.1.0'.", true),
	column("erlang_version", str, "Erlang/OTP version.", true),
	column("management_version", str, "Management plugin version.", true),
	column("product_name", str, "Product name ('RabbitMQ' or a distribution's name).", true),
	column("product_version", str, "Product version.", true),
	column("rates_mode", str, "Statistics rates mode: none, basic or detailed.", true),
	column("default_queue_type", str, "Cluster default queue type for new queues (classic, quorum, stream).", true),
	column("messages", i64, "Messages in all queues (ready + unacknowledged).", true),
	column("messages_ready", i64, "Messages ready for delivery in all queues.", true),
	column("messages_unacknowledged", i64, "Messages delivered and awaiting acknowledgement.", true),
	column("publish_rate", f64, "Messages published per second (recent sample).", true),
	column("deliver_get_rate", f64, "Messages delivered or fetched per second.", true),
	column("ack_rate", f64, "Acknowledgements per second.", true),
	column("connections", i64, "Open client connections.", true),
	column("channels", i64, "Open channels.", true),
	column("exchanges", i64, "Exchanges (all vhosts).", true),
	column("queues", i64, "Queues (all vhosts).", true),
	column("consumers", i64, "Consumers (all vhosts).", true),
	column("listeners", strList, "Listening protocols and ports as 'protocol:port@node'.", false),
}

var overviewExamples = []vgi.CatalogExample{
	{SQL: "SELECT cluster_name, rabbitmq_version, messages_ready, messages_unacknowledged, queues, connections FROM rabbitmq.main.overview()",
		Description: "Cluster name, version and message backlog at a glance"},
	{SQL: "SELECT rabbitmq_version, erlang_version, len(listeners) AS listener_count FROM rabbitmq.main.overview() ORDER BY rabbitmq_version",
		Description: "Server and Erlang versions plus how many listeners are open"},
}

func newOverview() vgi.TableFunction {
	return newList(&listFunc[noArgs]{
		name: "overview", columns: overviewColumns,
		meta: meta(docs{Title: "Cluster Overview", Category: "broker",
			Keywords: "overview, cluster, version, totals, backlog, message rates, /api/overview",
			DocLLM: "One row from GET /api/overview: cluster_name, node, versions, rates_mode, default_queue_type, cluster-wide " +
				"message totals (messages, messages_ready, messages_unacknowledged), publish/deliver/ack rates, object counts " +
				"(connections, channels, exchanges, queues, consumers) and listeners. Named arg: secret_name. Use it for a health " +
				"summary or to read the broker version.",
			DocMD: "## overview()\n\nOne row summarising the cluster: versions, message totals, rates and object counts."},
			"Cluster-wide summary: versions, message totals, rates and object counts", overviewColumns, overviewExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs, _ int64) ([][]any, bool, error) {
			v, err := c.getJSON(ctx, "/overview", nil)
			if err != nil {
				return nil, false, err
			}
			m, _ := v.(map[string]any)
			listeners := []string{}
			if ls, ok := m["listeners"].([]any); ok {
				for _, l := range ls {
					if lm, ok := l.(map[string]any); ok {
						listeners = append(listeners, fmt.Sprintf("%s:%s@%s", renderValue(lm["protocol"]), renderValue(lm["port"]), renderValue(lm["node"])))
					}
				}
			}
			sort.Strings(listeners)
			return [][]any{{
				vStrNE(m["cluster_name"]), vStrNE(m["node"]), vStr(m["rabbitmq_version"]), vStr(m["erlang_version"]),
				vStr(m["management_version"]), vStr(m["product_name"]), vStr(m["product_version"]), vStr(m["rates_mode"]),
				vStr(m["default_queue_type"]),
				vInt(at(m, "queue_totals", "messages")), vInt(at(m, "queue_totals", "messages_ready")),
				vInt(at(m, "queue_totals", "messages_unacknowledged")),
				rate(m, "message_stats", "publish"), rate(m, "message_stats", "deliver_get"), rate(m, "message_stats", "ack"),
				vInt(at(m, "object_totals", "connections")), vInt(at(m, "object_totals", "channels")),
				vInt(at(m, "object_totals", "exchanges")), vInt(at(m, "object_totals", "queues")),
				vInt(at(m, "object_totals", "consumers")), listeners,
			}}, false, nil
		},
	})
}

// ---------------------------------------------------------------- nodes

var nodesColumns = []arrow.Field{
	column("name", str, "Node name, e.g. 'rabbit@host'.", false),
	column("type", str, "Node type ('disc' or 'ram').", true),
	column("running", boolean, "Whether the node is running.", false),
	column("uptime_ms", i64, "Milliseconds since the node started.", true),
	column("mem_used", i64, "Memory in use, bytes.", true),
	column("mem_limit", i64, "Memory high watermark, bytes; publishers block above it.", true),
	column("mem_alarm", boolean, "Whether the memory alarm is in effect (publishers blocked).", true),
	column("disk_free", i64, "Free disk space, bytes.", true),
	column("disk_free_limit", i64, "Free-disk low watermark, bytes.", true),
	column("disk_free_alarm", boolean, "Whether the disk alarm is in effect (publishers blocked).", true),
	column("fd_used", i64, "File descriptors in use.", true),
	column("fd_total", i64, "File descriptors available.", true),
	column("sockets_used", i64, "Sockets in use.", true),
	column("sockets_total", i64, "Sockets available.", true),
	column("proc_used", i64, "Erlang processes in use.", true),
	column("proc_total", i64, "Erlang process limit.", true),
	column("processors", i64, "CPU cores the runtime sees.", true),
	column("run_queue", i64, "Erlang scheduler run queue length.", true),
	column("partitions", strList, "Nodes this node cannot see (network partition); empty when healthy.", false),
	column("being_drained", boolean, "Whether the node is in maintenance mode.", true),
	column("rabbitmq_version", str, "RabbitMQ version on this node.", true),
	column("erlang_version", str, "Erlang/OTP version on this node.", true),
	column("enabled_plugins", strList, "Plugins enabled on this node.", false),
}

var nodesExamples = []vgi.CatalogExample{
	{SQL: "SELECT name, mem_alarm, disk_free_alarm, round(100.0 * mem_used / mem_limit, 1) AS mem_pct, fd_used, len(partitions) AS partitions FROM rabbitmq.main.nodes() ORDER BY name",
		Description: "Resource alarms, memory headroom and partitions per node"},
	{SQL: "SELECT name FROM rabbitmq.main.nodes() WHERE mem_alarm OR disk_free_alarm OR len(partitions) > 0 OR NOT running ORDER BY name",
		Description: "Nodes that need attention"},
}

func newNodes() vgi.TableFunction {
	return newList(&listFunc[noArgs]{
		name: "nodes", columns: nodesColumns,
		meta: meta(docs{Title: "Cluster Nodes", Category: "broker",
			Keywords: "nodes, memory alarm, disk alarm, file descriptors, partitions, cluster health, /api/nodes",
			DocLLM: "One row per cluster node from GET /api/nodes: running, uptime_ms, memory used/limit/alarm, disk free/limit/alarm, " +
				"file descriptors, sockets, Erlang processes, partitions (`VARCHAR[]` of unreachable nodes), maintenance mode, " +
				"versions and enabled plugins. Named arg: secret_name. An alarm column true means publishers are blocked.",
			DocMD: "## nodes()\n\nOne row per node with resource usage, memory/disk alarms and network partitions."},
			"Cluster nodes with memory, disk and file-descriptor usage, alarms and partitions", nodesColumns, nodesExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, "/nodes", nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				return []any{vStr(m["name"]), vStr(m["type"]), vBoolF(m["running"]), vInt(m["uptime"]),
					vInt(m["mem_used"]), vInt(m["mem_limit"]), vBool(m["mem_alarm"]),
					vInt(m["disk_free"]), vInt(m["disk_free_limit"]), vBool(m["disk_free_alarm"]),
					vInt(m["fd_used"]), vInt(m["fd_total"]), vInt(m["sockets_used"]), vInt(m["sockets_total"]),
					vInt(m["proc_used"]), vInt(m["proc_total"]), vInt(m["processors"]), vInt(m["run_queue"]),
					vListE(m["partitions"]), vBool(m["being_drained"]), vStr(m["rabbitmq_version"]), vStr(m["erlang_version"]),
					vListE(m["enabled_plugins"])}
			}), false, nil
		},
	})
}

// ---------------------------------------------------------------- vhosts

var vhostsColumns = []arrow.Field{
	column("name", str, "Virtual host name ('/' is the default vhost).", false),
	column("description", str, "Description from the vhost metadata.", true),
	column("tags", strList, "Vhost tags.", false),
	column("default_queue_type", str, "Queue type new queues get when they do not set x-queue-type.", true),
	column("tracing", boolean, "Whether the firehose tracer is on.", true),
	column("protected_from_deletion", boolean, "Whether the vhost is protected from deletion.", true),
	column("cluster_state", strMap, "Per-node state of the vhost, e.g. {'rabbit@host': 'running'}.", false),
	column("messages", i64, "Messages in the vhost's queues.", true),
	column("messages_ready", i64, "Ready messages in the vhost's queues.", true),
	column("messages_unacknowledged", i64, "Unacknowledged messages in the vhost's queues.", true),
	column("publish_rate", f64, "Messages published per second into the vhost.", true),
	column("deliver_get_rate", f64, "Messages delivered or fetched per second.", true),
}

var vhostsExamples = []vgi.CatalogExample{
	{SQL: "SELECT name, default_queue_type, messages_ready, messages_unacknowledged FROM rabbitmq.main.vhosts() ORDER BY messages_ready DESC NULLS LAST, name",
		Description: "Virtual hosts ordered by backlog"},
	{SQL: "SELECT name, map_keys(cluster_state) AS nodes FROM rabbitmq.main.vhosts() WHERE NOT list_bool_and(list_transform(map_values(cluster_state), s -> s = 'running')) ORDER BY name",
		Description: "Vhosts that are not running on every node"},
}

func newVhosts() vgi.TableFunction {
	return newList(&listFunc[noArgs]{
		name: "vhosts", columns: vhostsColumns,
		meta: meta(docs{Title: "Virtual Hosts", Category: "broker",
			Keywords: "vhosts, virtual hosts, tenants, default queue type, /api/vhosts",
			DocLLM: "One row per virtual host from GET /api/vhosts: name, description, tags, default_queue_type, tracing, " +
				"protected_from_deletion, cluster_state `MAP` (node to state), message totals and rates. Named arg: secret_name. " +
				"Vhost names are the `vhost :=` argument of other functions.",
			DocMD: "## vhosts()\n\nEvery virtual host with its metadata, per-node state and message totals."},
			"Virtual hosts with metadata, per-node state and message totals", vhostsColumns, vhostsExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, "/vhosts", nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				desc := vStrNE(m["description"])
				if desc == nil {
					desc = vStrNE(at(m, "metadata", "description"))
				}
				dqt := vStrNE(m["default_queue_type"])
				if dqt == nil {
					dqt = vStrNE(at(m, "metadata", "default_queue_type"))
				}
				return []any{vStr(m["name"]), desc, vListE(m["tags"]), dqt, vBool(m["tracing"]), vBool(m["protected_from_deletion"]),
					vMap(m["cluster_state"]), vInt(m["messages"]), vInt(m["messages_ready"]), vInt(m["messages_unacknowledged"]),
					rate(m, "message_stats", "publish"), rate(m, "message_stats", "deliver_get")}
			}), false, nil
		},
	})
}

// ---------------------------------------------------------------- health_checks

type healthArgs struct {
	CertWithin int64   `vgi:"name=cert_within,default=1,ge=1,le=1000,doc=certificate_expiration fails when a listener certificate expires within this many cert_unit."`
	CertUnit   string  `vgi:"name=cert_unit,default=months,choices='days,weeks,months,years',doc=Unit of cert_within."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the rabbitmq secret to use when several exist."`
}

func (a healthArgs) secret() string { return deref(a.SecretName) }

var healthColumns = []arrow.Field{
	column("check", str, "Check name: alarms, local_alarms, certificate_expiration, node_is_quorum_critical, virtual_hosts, is_in_service, below_node_connection_limit, ready_to_serve_clients.", false),
	column("ok", boolean, "true when the check passed (HTTP 200).", false),
	column("status", str, "The check's status: 'ok' or 'failed'.", false),
	column("reason", str, "Why the check failed (or a note when it passed, e.g. 'single node cluster'); NULL otherwise.", true),
	column("http_status", i64, "HTTP status of the check endpoint (200 ok, 503 failed).", false),
	column("details", str, "The full check response as JSON (alarm lists, expiring certificates, critical queues).", false),
}

var healthExamples = []vgi.CatalogExample{
	{SQL: "SELECT check, status, reason FROM rabbitmq.main.health_checks() WHERE NOT ok ORDER BY check",
		Description: "Failing health checks and why"},
	{SQL: "SELECT bool_and(ok) AS healthy, count(*) FILTER (WHERE NOT ok) AS failing FROM rabbitmq.main.health_checks(cert_within := 30, cert_unit := 'days')",
		Description: "One healthy/unhealthy verdict, with certificates checked 30 days ahead"},
}

// healthChecks maps check names to their endpoints.
func healthChecks(a healthArgs) [][2]string {
	return [][2]string{
		{"alarms", "/health/checks/alarms"},
		{"local_alarms", "/health/checks/local-alarms"},
		{"certificate_expiration", "/health/checks/certificate-expiration/" + strconv.FormatInt(a.CertWithin, 10) + "/" + a.CertUnit},
		{"node_is_quorum_critical", "/health/checks/node-is-quorum-critical"},
		{"virtual_hosts", "/health/checks/virtual-hosts"},
		{"is_in_service", "/health/checks/is-in-service"},
		{"below_node_connection_limit", "/health/checks/below-node-connection-limit"},
		{"ready_to_serve_clients", "/health/checks/ready-to-serve-clients"},
	}
}

func newHealthChecks() vgi.TableFunction {
	return newList(&listFunc[healthArgs]{
		name: "health_checks", columns: healthColumns,
		meta: meta(docs{Title: "Health Checks", Category: "health",
			Keywords: "health checks, alarms, certificate expiry, quorum critical, readiness, /api/health/checks",
			DocLLM: "Runs the management health-check endpoints and returns one row per check: alarms (cluster-wide resource alarms), " +
				"local_alarms, certificate_expiration (listener certificates expiring within cert_within cert_unit), " +
				"node_is_quorum_critical (stopping this node would lose quorum), virtual_hosts, is_in_service, " +
				"below_node_connection_limit and ready_to_serve_clients. Columns: check, ok, status, reason, http_status, details JSON. " +
				"A failed check (HTTP 503) is a row with ok=false, never an error. Checks a broker does not have (older versions) are omitted. " +
				"Named args: cert_within (default 1), cert_unit (days|weeks|months|years, default months), secret_name.",
			DocMD: "## health_checks(cert_within := 1, cert_unit := 'months')\n\nEvery health check as a row; failures are rows with `ok = false`."},
			"Run the broker's health checks (alarms, certificate expiry, quorum criticality, readiness)", healthColumns, healthExamples),
		fetch: func(ctx context.Context, c *conn, a healthArgs, _ int64) ([][]any, bool, error) {
			rows := [][]any{}
			for _, hc := range healthChecks(a) {
				status, body, err := c.call(ctx, hc[1], nil, reqOpts{healthCheck: true})
				if err != nil {
					return nil, false, err
				}
				if status == http.StatusNotFound {
					continue // endpoint not present on this broker version
				}
				if status != http.StatusOK && status != http.StatusServiceUnavailable {
					return nil, false, errorFrom(hc[1], status, body)
				}
				v, err := decodeJSON(hc[1], body)
				if err != nil {
					return nil, false, err
				}
				m, _ := v.(map[string]any)
				st := vStrNE(m["status"])
				if st == nil {
					st = map[bool]string{true: "ok", false: "failed"}[status == http.StatusOK]
				}
				rows = append(rows, []any{hc[0], status == http.StatusOK, st, vStrNE(m["reason"]), int64(status), vJSON(v)})
			}
			return rows, false, nil
		},
	})
}

// ---------------------------------------------------------------- feature_flags

var featureFlagColumns = []arrow.Field{
	column("name", str, "Feature flag name.", false),
	column("state", str, "enabled, disabled or state_changing.", false),
	column("stability", str, "required, stable or experimental.", true),
	column("description", str, "What the flag enables.", true),
	column("doc_url", str, "Documentation link; NULL when none.", true),
	column("provided_by", str, "Application that defines the flag, e.g. 'rabbit'.", true),
}

var featureFlagExamples = []vgi.CatalogExample{
	{SQL: "SELECT name, stability FROM rabbitmq.main.feature_flags() WHERE state <> 'enabled' ORDER BY name",
		Description: "Feature flags not yet enabled (must be enabled before upgrading)"},
	{SQL: "SELECT stability, state, count(*) AS flags FROM rabbitmq.main.feature_flags() GROUP BY ALL ORDER BY stability, state",
		Description: "Feature flags by stability and state"},
}

func newFeatureFlags() vgi.TableFunction {
	return newList(&listFunc[noArgs]{
		name: "feature_flags", columns: featureFlagColumns,
		meta: meta(docs{Title: "Feature Flags", Category: "health",
			Keywords: "feature flags, upgrade readiness, stability, /api/feature-flags",
			DocLLM: "One row per feature flag from GET /api/feature-flags: name, state (enabled/disabled), stability (required/stable/experimental), " +
				"description, doc_url, provided_by. Named arg: secret_name. Before an upgrade every stable flag should be enabled.",
			DocMD: "## feature_flags()\n\nEvery feature flag and whether it is enabled; check before upgrading."},
			"Feature flags with state and stability (upgrade readiness)", featureFlagColumns, featureFlagExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, "/feature-flags", nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				return []any{vStr(m["name"]), vStr(m["state"]), vStrNE(m["stability"]), vStrNE(m["desc"]), vStrNE(m["doc_url"]), vStrNE(m["provided_by"])}
			}), false, nil
		},
	})
}

// ---------------------------------------------------------------- deprecated_features

var deprecatedColumns = []arrow.Field{
	column("name", str, "Deprecated feature name, e.g. 'transient_nonexcl_queues'.", false),
	column("deprecation_phase", str, "permitted_by_default, denied_by_default, disconnected or removed.", true),
	column("state", str, "Whether the feature is currently permitted or denied on this broker.", true),
	column("description", str, "What the feature is.", true),
	column("doc_url", str, "Documentation link; NULL when none.", true),
	column("provided_by", str, "Application that defines the feature.", true),
	column("in_use", boolean, "true when something on the broker uses the feature right now (/api/deprecated-features/used).", false),
}

var deprecatedExamples = []vgi.CatalogExample{
	{SQL: "SELECT name, deprecation_phase, doc_url FROM rabbitmq.main.deprecated_features() WHERE in_use ORDER BY name",
		Description: "Deprecated features in use that block a future upgrade"},
	{SQL: "SELECT deprecation_phase, count(*) AS features FROM rabbitmq.main.deprecated_features() GROUP BY deprecation_phase ORDER BY deprecation_phase",
		Description: "Deprecated features by phase"},
}

func newDeprecatedFeatures() vgi.TableFunction {
	return newList(&listFunc[noArgs]{
		name: "deprecated_features", columns: deprecatedColumns,
		meta: meta(docs{Title: "Deprecated Features", Category: "health",
			Keywords: "deprecated features, upgrade blockers, in use, /api/deprecated-features",
			DocLLM: "One row per deprecated feature from GET /api/deprecated-features, with in_use = true for features listed by " +
				"/api/deprecated-features/used (something uses it now, e.g. transient non-exclusive queues or global QoS). " +
				"Columns: name, deprecation_phase, state, description, doc_url, provided_by, in_use. Named arg: secret_name.",
			DocMD: "## deprecated_features()\n\nDeprecated features, their phase and whether anything uses them now."},
			"Deprecated features, their phase and whether they are in use", deprecatedColumns, deprecatedExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs, _ int64) ([][]any, bool, error) {
			all, err := c.getList(ctx, "/deprecated-features", nil)
			if err != nil {
				return nil, false, err
			}
			used, err := c.getList(ctx, "/deprecated-features/used", nil)
			if err != nil {
				return nil, false, err
			}
			inUse := map[string]bool{}
			for _, u := range used {
				if n, ok := u["name"].(string); ok {
					inUse[n] = true
				}
			}
			return mapRows(all, func(m map[string]any) []any {
				n, _ := m["name"].(string)
				return []any{n, vStrNE(m["deprecation_phase"]), vStrNE(m["state"]), vStrNE(m["desc"]), vStrNE(m["doc_url"]),
					vStrNE(m["provided_by"]), inUse[n]}
			}), false, nil
		},
	})
}

// ---------------------------------------------------------------- definitions_summary

var definitionsColumns = []arrow.Field{
	column("object_type", str, "Definition section: users, vhosts, permissions, topic_permissions, parameters, global_parameters, policies, queues, exchanges, bindings.", false),
	column("object_count", i64, "Number of objects of that type in the exported definitions.", false),
}

var definitionsExamples = []vgi.CatalogExample{
	{SQL: "SELECT object_type, object_count FROM rabbitmq.main.definitions_summary() ORDER BY object_type",
		Description: "How many of each object the broker's definitions export holds"},
	{SQL: "SELECT object_type, object_count FROM rabbitmq.main.definitions_summary(vhost := 'shop') WHERE object_count > 0 ORDER BY object_count DESC, object_type",
		Description: "Object counts for one vhost's definitions"},
}

// definitionSections are counted in this order.
var definitionSections = []string{"users", "vhosts", "permissions", "topic_permissions", "parameters", "global_parameters",
	"policies", "queues", "exchanges", "bindings"}

func newDefinitionsSummary() vgi.TableFunction {
	return newList(&listFunc[vhostArgs]{
		name: "definitions_summary", columns: definitionsColumns,
		meta: meta(docs{Title: "Definitions Summary", Category: "config",
			Keywords: "definitions, export, backup, object counts, schema, /api/definitions",
			DocLLM: "Counts the objects in the broker's definitions export (GET /api/definitions, or /api/definitions/{vhost} with " +
				"vhost :=), one row per section: object_type, object_count. Only counts are returned; the export's password hashes " +
				"and parameter values never leave the worker. Use it to compare brokers or check an import.",
			DocMD: "## definitions_summary(vhost := NULL)\n\nObject counts per section of the definitions export (no secrets returned)."},
			"Object counts in the broker's definitions export, optionally for one vhost", definitionsColumns, definitionsExamples),
		fetch: func(ctx context.Context, c *conn, a vhostArgs, _ int64) ([][]any, bool, error) {
			v, err := c.getJSON(ctx, vhostPath("/definitions", a.Vhost), nil)
			if err != nil {
				return nil, false, err
			}
			m, _ := v.(map[string]any)
			rows := [][]any{}
			for _, s := range definitionSections {
				arr, ok := m[s].([]any)
				if !ok {
					continue
				}
				rows = append(rows, []any{s, int64(len(arr))})
			}
			return rows, false, nil
		},
	})
}
