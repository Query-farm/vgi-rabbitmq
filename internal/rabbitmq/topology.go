// Topology functions: queues, exchanges, bindings, policies,
// operator_policies, parameters, global_parameters and shovels.

package rabbitmq

import (
	"context"
	"net/url"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// pagedArgs are the arguments of the paginated listings (queues, exchanges,
// connections, channels): a vhost scope pushed into the path, a name filter
// pushed as ?name=&use_regex=, and the page size.
type pagedArgs struct {
	Vhost      *string `vgi:"name=vhost,doc=Only this virtual host (pushed into the API path); default all vhosts."`
	Name       *string `vgi:"name=name,doc=Name filter pushed to the API: a case-insensitive substring or with use_regex := true a regular expression."`
	UseRegex   bool    `vgi:"name=use_regex,default=false,doc=Treat name as a regular expression (the broker's use_regex=true)."`
	PageSize   int64   `vgi:"name=page_size,default=100,ge=1,le=500,doc=Objects per API page (the management API caps pages at 500)."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the rabbitmq secret to use when several exist."`
}

func (a pagedArgs) secret() string { return deref(a.SecretName) }

// fetchPaged fetches one page of a paginated listing and maps it.
func fetchPaged(ctx context.Context, c *conn, path string, a pagedArgs, pageNum int64, row func(map[string]any) []any) ([][]any, bool, error) {
	params := url.Values{}
	if a.Name != nil && *a.Name != "" {
		params.Set("name", *a.Name)
		if a.UseRegex {
			params.Set("use_regex", "true")
		}
	}
	p, err := c.getPage(ctx, path, params, pageNum, a.PageSize)
	if err != nil {
		return nil, false, err
	}
	return mapRows(p.Items, row), pageNum < p.PageCount, nil
}

const pagedDoc = "Named args: vhost (pushed into the path), name (case-insensitive substring pushed as ?name=, or a regex with " +
	"use_regex := true), page_size (default 100, max 500; pages are fetched one per batch with the cursor externalized), secret_name."

// ---------------------------------------------------------------- queues

var queuesColumns = []arrow.Field{
	column("vhost", str, "Virtual host.", false),
	column("name", str, "Queue name.", false),
	column("type", str, "Queue type: classic, quorum or stream.", false),
	column("state", str, "running, idle, flow, down, minority, ...", true),
	column("durable", boolean, "Survives a broker restart.", false),
	column("auto_delete", boolean, "Deleted when its last consumer unsubscribes.", false),
	column("exclusive", boolean, "Owned by one connection.", false),
	column("node", str, "Node hosting the queue (the leader for quorum queues and streams).", true),
	column("members", strList, "Replica nodes of a quorum queue or stream; NULL for classic queues.", true),
	column("policy", str, "Name of the policy applied to the queue; NULL when none.", true),
	column("operator_policy", str, "Name of the operator policy applied; NULL when none.", true),
	column("effective_policy_definition", strMap, "The merged policy settings in effect (values as text).", false),
	column("arguments", strMap, "Declared x-arguments, e.g. arguments['x-queue-type'] (values as text).", false),
	column("messages", i64, "Messages in the queue (ready + unacknowledged); for streams, messages stored.", true),
	column("messages_ready", i64, "Messages ready for delivery: the backlog consumers have not received (the queue's lag).", true),
	column("messages_unacknowledged", i64, "Messages delivered and not yet acknowledged (in flight).", true),
	column("message_bytes", i64, "Bytes of message bodies in the queue.", true),
	column("consumers", i64, "Number of consumers.", true),
	column("consumer_capacity", f64, "Fraction of time the queue could deliver to consumers immediately (0 to 1).", true),
	column("publish_rate", f64, "Messages published per second into the queue.", true),
	column("deliver_get_rate", f64, "Messages delivered or fetched per second.", true),
	column("ack_rate", f64, "Acknowledgements per second.", true),
	column("redeliver_rate", f64, "Redeliveries per second.", true),
	column("memory", i64, "Bytes of memory used by the queue process.", true),
	column("head_message_timestamp", tstz, "AMQP timestamp of the oldest message (classic queues; needs publishers to set it).", true),
	column("first_timestamp", tstz, "Timestamp of the oldest message still stored in a stream.", true),
	column("idle_since", tstz, "When the queue went idle; NULL when active.", true),
	column("delivery_limit", i64, "Quorum queue delivery limit (poison-message handling); NULL otherwise.", true),
}

var queuesExamples = []vgi.CatalogExample{
	{SQL: "SELECT vhost, name, type, messages_ready, messages_unacknowledged, consumers FROM rabbitmq.main.queues() WHERE messages_ready > 0 ORDER BY messages_ready DESC, vhost, name",
		Description: "Queues with a backlog, largest first"},
	{SQL: "SELECT name, type, arguments['x-dead-letter-exchange'] AS dlx, policy FROM rabbitmq.main.queues(vhost := 'shop', name := '^orders\\.', use_regex := true) ORDER BY name",
		Description: "Order queues in the shop vhost (vhost and regex pushed to the API) with their dead-letter exchange"},
	{SQL: "SELECT vhost, name FROM rabbitmq.main.queues() WHERE consumers = 0 AND messages_ready > 0 ORDER BY vhost, name",
		Description: "Queues piling up with no consumer"},
}

func newQueues() vgi.TableFunction {
	return newList(&listFunc[pagedArgs]{
		name: "queues", columns: queuesColumns,
		meta: meta(docs{Title: "Queues", Category: "topology",
			Keywords: "queues, backlog, messages ready, unacknowledged, consumers, quorum, stream, classic, lag, /api/queues",
			DocLLM: "One row per queue from GET /api/queues[/{vhost}] (paginated): vhost, name, type (classic/quorum/stream), state, " +
				"durability flags, node/members, policy, operator_policy, effective_policy_definition and arguments (`MAP`, values as text), " +
				"messages, messages_ready (backlog/lag), messages_unacknowledged (in flight), message_bytes, consumers, consumer_capacity, " +
				"publish/deliver_get/ack/redeliver rates, memory, head_message_timestamp, first_timestamp (streams), idle_since, delivery_limit. " +
				pagedDoc,
			DocMD: "## queues(vhost := NULL, name := NULL, use_regex := false, page_size := 100)\n\nEvery queue with depth, consumers, rates, type, policy and arguments. `vhost` and `name` are pushed to the API."},
			"Queues with depth, consumers, rates, type, policy and arguments; vhost and name filters push down", queuesColumns, queuesExamples),
		fetch: func(ctx context.Context, c *conn, a pagedArgs, pg int64) ([][]any, bool, error) {
			return fetchPaged(ctx, c, vhostPath("/queues", a.Vhost), a, pg, queueRow)
		},
	})
}

func queueRow(m map[string]any) []any {
	node := vStrNE(m["leader"])
	if node == nil {
		node = vStrNE(m["node"])
	}
	typ := vStrNE(m["type"])
	if typ == nil {
		typ = "classic"
	}
	return []any{vStr(m["vhost"]), vStr(m["name"]), typ, vStrNE(m["state"]),
		vBoolF(m["durable"]), vBoolF(m["auto_delete"]), vBoolF(m["exclusive"]), node, vList(m["members"]),
		vStrNE(m["policy"]), vStrNE(m["operator_policy"]), vMap(m["effective_policy_definition"]), vMap(m["arguments"]),
		vInt(m["messages"]), vInt(m["messages_ready"]), vInt(m["messages_unacknowledged"]), vInt(m["message_bytes"]),
		vInt(m["consumers"]), vFloat(m["consumer_capacity"]),
		rate(m, "message_stats", "publish"), rate(m, "message_stats", "deliver_get"), rate(m, "message_stats", "ack"),
		rate(m, "message_stats", "redeliver"), vInt(m["memory"]),
		vEpochS(m["head_message_timestamp"]), vEpochMs(m["first_timestamp"]), vTime(m["idle_since"]), vInt(m["delivery_limit"])}
}

// ---------------------------------------------------------------- exchanges

var exchangesColumns = []arrow.Field{
	column("vhost", str, "Virtual host.", false),
	column("name", str, "Exchange name; '' is the default exchange.", false),
	column("type", str, "direct, fanout, topic, headers, or a plugin type (x-...).", false),
	column("durable", boolean, "Survives a broker restart.", false),
	column("auto_delete", boolean, "Deleted when its last binding is removed.", false),
	column("internal", boolean, "Clients cannot publish to it directly (exchange-to-exchange only).", false),
	column("arguments", strMap, "Declared arguments, e.g. alternate-exchange (values as text).", false),
	column("policy", str, "Policy applied to the exchange; NULL when none.", true),
	column("publish_in", i64, "Messages published into the exchange (since stats reset).", true),
	column("publish_out", i64, "Messages routed out of the exchange.", true),
	column("publish_in_rate", f64, "Messages published in per second.", true),
	column("publish_out_rate", f64, "Messages routed out per second.", true),
}

var exchangesExamples = []vgi.CatalogExample{
	{SQL: "SELECT vhost, name, type FROM rabbitmq.main.exchanges(vhost := 'shop') WHERE name NOT LIKE 'amq.%' AND name <> '' ORDER BY name",
		Description: "User-declared exchanges in one vhost"},
	{SQL: "SELECT type, count(*) AS exchanges FROM rabbitmq.main.exchanges() GROUP BY type ORDER BY exchanges DESC, type",
		Description: "Exchanges by type"},
}

func newExchanges() vgi.TableFunction {
	return newList(&listFunc[pagedArgs]{
		name: "exchanges", columns: exchangesColumns,
		meta: meta(docs{Title: "Exchanges", Category: "topology",
			Keywords: "exchanges, routing, topic, fanout, headers, direct, /api/exchanges",
			DocLLM: "One row per exchange from GET /api/exchanges[/{vhost}] (paginated): vhost, name ('' = default exchange), type, " +
				"durable, auto_delete, internal, arguments `MAP`, policy, publish_in/publish_out counts and rates. " + pagedDoc,
			DocMD: "## exchanges(vhost := NULL, name := NULL, use_regex := false, page_size := 100)\n\nEvery exchange with type, flags and publish rates."},
			"Exchanges with type, flags, arguments and publish rates; vhost and name filters push down", exchangesColumns, exchangesExamples),
		fetch: func(ctx context.Context, c *conn, a pagedArgs, pg int64) ([][]any, bool, error) {
			return fetchPaged(ctx, c, vhostPath("/exchanges", a.Vhost), a, pg, func(m map[string]any) []any {
				return []any{vStr(m["vhost"]), vStr(m["name"]), vStr(m["type"]), vBoolF(m["durable"]), vBoolF(m["auto_delete"]),
					vBoolF(m["internal"]), vMap(m["arguments"]), vStrNE(m["policy"]),
					vInt(at(m, "message_stats", "publish_in")), vInt(at(m, "message_stats", "publish_out")),
					rate(m, "message_stats", "publish_in"), rate(m, "message_stats", "publish_out")}
			})
		},
	})
}

// ---------------------------------------------------------------- bindings

var bindingsColumns = []arrow.Field{
	column("vhost", str, "Virtual host.", false),
	column("source", str, "Source exchange; '' is the default exchange (every queue's implicit binding).", false),
	column("destination", str, "Destination queue or exchange.", false),
	column("destination_type", str, "'queue' or 'exchange' (exchange-to-exchange binding).", false),
	column("routing_key", str, "Binding key (topic pattern, direct key, or '' for fanout/headers).", false),
	column("arguments", strMap, "Binding arguments, e.g. x-match for headers exchanges (values as text).", false),
	column("properties_key", str, "The binding's identifier in the API (routing key plus argument hash).", true),
}

var bindingsExamples = []vgi.CatalogExample{
	{SQL: "SELECT source, routing_key, destination_type, destination FROM rabbitmq.main.bindings(vhost := 'shop') WHERE source <> '' ORDER BY source, destination",
		Description: "The routing graph of one vhost (default-exchange bindings left out)"},
	{SQL: "SELECT q.vhost, q.name FROM rabbitmq.main.queues() q ANTI JOIN (SELECT vhost, destination FROM rabbitmq.main.bindings() WHERE source <> '' AND destination_type = 'queue') b ON b.vhost = q.vhost AND b.destination = q.name ORDER BY q.vhost, q.name",
		Description: "Queues no exchange routes to (only reachable through the default exchange)"},
}

func newBindings() vgi.TableFunction {
	return newList(&listFunc[vhostArgs]{
		name: "bindings", columns: bindingsColumns,
		meta: meta(docs{Title: "Bindings", Category: "topology",
			Keywords: "bindings, routing graph, routing key, topology, exchange to queue, /api/bindings",
			DocLLM: "One row per binding (an edge of the routing graph) from GET /api/bindings[/{vhost}]: vhost, source exchange " +
				"('' = default exchange), destination, destination_type (queue/exchange), routing_key, arguments `MAP`, properties_key. " +
				"Named args: vhost (pushed into the path), secret_name. Join with queues/exchanges to find unrouted queues or follow routes.",
			DocMD: "## bindings(vhost := NULL)\n\nEvery binding: the edges of the exchange-to-queue routing graph."},
			"Bindings: the routing graph from exchanges to queues and exchanges", bindingsColumns, bindingsExamples),
		fetch: func(ctx context.Context, c *conn, a vhostArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, vhostPath("/bindings", a.Vhost), nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				return []any{vStr(m["vhost"]), vStr(m["source"]), vStr(m["destination"]), vStr(m["destination_type"]),
					vStr(m["routing_key"]), vMap(m["arguments"]), vStrNE(m["properties_key"])}
			}), false, nil
		},
	})
}

// ---------------------------------------------------------------- policies / operator_policies

var policyColumns = []arrow.Field{
	column("vhost", str, "Virtual host.", false),
	column("name", str, "Policy name.", false),
	column("pattern", str, "Regular expression matched against queue/exchange names.", false),
	column("apply_to", str, "queues, exchanges, all, classic_queues, quorum_queues or streams.", false),
	column("priority", i64, "Priority; the highest-priority matching policy wins.", false),
	column("definition", strMap, "Policy keys and values, e.g. definition['message-ttl'] (values as text).", false),
}

var policyExamples = []vgi.CatalogExample{
	{SQL: "SELECT vhost, name, pattern, apply_to, definition['message-ttl'] AS ttl_ms FROM rabbitmq.main.policies() ORDER BY vhost, priority DESC, name",
		Description: "Policies with their message TTL"},
	{SQL: "SELECT p.name AS policy, q.name AS queue FROM rabbitmq.main.policies(vhost := 'shop') p JOIN rabbitmq.main.queues(vhost := 'shop') q ON q.policy = p.name ORDER BY p.name, q.name",
		Description: "Which queues each policy applies to"},
}

var operatorPolicyExamples = []vgi.CatalogExample{
	{SQL: "SELECT vhost, name, pattern, definition FROM rabbitmq.main.operator_policies() ORDER BY vhost, name",
		Description: "Operator policies (limits users cannot override)"},
	{SQL: "SELECT vhost, count(*) AS operator_policies FROM rabbitmq.main.operator_policies() GROUP BY vhost ORDER BY vhost",
		Description: "Operator policies per vhost"},
}

func policyRow(m map[string]any) []any {
	pri := vInt(m["priority"])
	if pri == nil {
		pri = int64(0)
	}
	return []any{vStr(m["vhost"]), vStr(m["name"]), vStr(m["pattern"]), vStr(m["apply-to"]), pri, vMap(m["definition"])}
}

func newPolicies(name, path, title, docLLM, docMD, desc string, ex []vgi.CatalogExample) vgi.TableFunction {
	return newList(&listFunc[vhostArgs]{
		name: name, columns: policyColumns,
		meta: meta(docs{Title: title, Category: "config",
			Keywords: "policies, ttl, max-length, dead lettering, ha, queue settings, " + path,
			DocLLM:   docLLM + " Columns: vhost, name, pattern, apply_to, priority, definition `MAP` (values as text). Named args: vhost (pushed into the path), secret_name.",
			DocMD:    docMD}, desc, policyColumns, ex),
		fetch: func(ctx context.Context, c *conn, a vhostArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, vhostPath(path, a.Vhost), nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, policyRow), false, nil
		},
	})
}

func newUserPolicies() vgi.TableFunction {
	return newPolicies("policies", "/policies", "Policies",
		"One row per user policy from GET /api/policies[/{vhost}].",
		"## policies(vhost := NULL)\n\nUser policies: pattern, target, priority and settings.",
		"User policies with pattern, target, priority and definition", policyExamples)
}

func newOperatorPolicies() vgi.TableFunction {
	return newPolicies("operator_policies", "/operator-policies", "Operator Policies",
		"One row per operator policy (limits set by the operator that user policies cannot exceed) from GET /api/operator-policies[/{vhost}].",
		"## operator_policies(vhost := NULL)\n\nOperator policies: limits users cannot override.",
		"Operator policies (operator-enforced limits) with pattern, target, priority and definition", operatorPolicyExamples)
}

// ---------------------------------------------------------------- parameters

type parametersArgs struct {
	Component  *string `vgi:"name=component,doc=Only this component (pushed into the path), e.g. 'shovel' or 'federation-upstream'."`
	Vhost      *string `vgi:"name=vhost,doc=Only this virtual host (needs component); pushed into the path."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the rabbitmq secret to use when several exist."`
}

func (a parametersArgs) secret() string { return deref(a.SecretName) }

var parametersColumns = []arrow.Field{
	column("vhost", str, "Virtual host.", false),
	column("component", str, "Component: shovel, federation-upstream, federation-upstream-set, vhost-limits, ...", false),
	column("name", str, "Parameter name.", false),
	column("value", str, "Parameter value as JSON, with every URI password, password query parameter and credential-named key replaced by '****'.", false),
}

var parametersExamples = []vgi.CatalogExample{
	{SQL: "SELECT vhost, name, json_extract_string(value, '$.\"src-queue\"') AS src_queue, json_extract_string(value, '$.\"dest-uri\"') AS dest_uri FROM rabbitmq.main.parameters(component := 'shovel') ORDER BY vhost, name",
		Description: "Dynamic shovels and where they move messages (credentials masked)"},
	{SQL: "SELECT component, count(*) AS parameters FROM rabbitmq.main.parameters() GROUP BY component ORDER BY component",
		Description: "Runtime parameters by component"},
}

func newParameters() vgi.TableFunction {
	return newList(&listFunc[parametersArgs]{
		name: "parameters", columns: parametersColumns,
		validate: func(a parametersArgs) error {
			if a.Vhost != nil && a.Component == nil {
				return errInvalid("parameters: vhost := needs component := as well (the API path is /api/parameters/{component}/{vhost})")
			}
			return nil
		},
		meta: meta(docs{Title: "Runtime Parameters", Category: "config",
			Keywords: "parameters, shovels, federation upstreams, vhost limits, runtime parameters, /api/parameters",
			DocLLM: "One row per vhost-scoped runtime parameter from GET /api/parameters[/{component}[/{vhost}]]: dynamic shovels " +
				"(component 'shovel'), federation upstreams ('federation-upstream'), vhost limits and others. Columns: vhost, component, " +
				"name, value (JSON). Credentials are masked: user:password@ in any URI becomes user:****@, password-like query parameters " +
				"and keys become '****'. Named args: component, vhost (needs component), secret_name. Read fields with json_extract_string(value, '$.\"src-uri\"').",
			DocMD: "## parameters(component := NULL, vhost := NULL)\n\nShovel, federation and other runtime parameters, with credentials in URIs masked."},
			"Runtime parameters (shovels, federation upstreams, limits) with credentials masked", parametersColumns, parametersExamples),
		fetch: func(ctx context.Context, c *conn, a parametersArgs, _ int64) ([][]any, bool, error) {
			path := "/parameters"
			if a.Component != nil {
				path += "/" + seg(*a.Component)
				if a.Vhost != nil {
					path += "/" + seg(*a.Vhost)
				}
			}
			items, err := c.getList(ctx, path, nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				return []any{vStr(m["vhost"]), vStr(m["component"]), vStr(m["name"]), vJSON(maskValue(m["value"]))}
			}), false, nil
		},
	})
}

var globalParametersColumns = []arrow.Field{
	column("name", str, "Global parameter name, e.g. 'cluster_name' or 'internal_cluster_id'.", false),
	column("value", str, "Value as JSON, with URI passwords and credential-named keys masked.", false),
}

var globalParametersExamples = []vgi.CatalogExample{
	{SQL: "SELECT name, value FROM rabbitmq.main.global_parameters() WHERE name = 'cluster_name'",
		Description: "The cluster name as stored in the global parameters"},
	{SQL: "SELECT name FROM rabbitmq.main.global_parameters() ORDER BY name",
		Description: "Every global parameter's name"},
}

func newGlobalParameters() vgi.TableFunction {
	return newList(&listFunc[noArgs]{
		name: "global_parameters", columns: globalParametersColumns,
		meta: meta(docs{Title: "Global Parameters", Category: "config",
			Keywords: "global parameters, cluster name, cluster id, /api/global-parameters",
			DocLLM: "One row per global (cluster-wide) runtime parameter from GET /api/global-parameters: name, value (JSON, credentials masked). " +
				"Named arg: secret_name.",
			DocMD: "## global_parameters()\n\nCluster-wide runtime parameters such as cluster_name."},
			"Cluster-wide runtime parameters such as cluster_name", globalParametersColumns, globalParametersExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, "/global-parameters", nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				return []any{vStr(m["name"]), vJSON(maskValue(m["value"]))}
			}), false, nil
		},
	})
}

// ---------------------------------------------------------------- shovels

var shovelsColumns = []arrow.Field{
	column("vhost", str, "Virtual host of the shovel.", false),
	column("name", str, "Shovel name.", false),
	column("type", str, "'dynamic' (a runtime parameter) or 'static' (configuration file).", true),
	column("state", str, "starting, running, blocked or terminated.", true),
	column("reason", str, "Why a shovel is blocked or terminated; NULL otherwise.", true),
	column("node", str, "Node running the shovel.", true),
	column("since", tstz, "When the shovel entered its state.", true),
	column("src_uri", str, "Source URI (credentials masked); NULL unless running.", true),
	column("src_queue", str, "Source queue (or exchange) name; NULL unless running.", true),
	column("dest_uri", str, "Destination URI (credentials masked); NULL unless running.", true),
	column("dest_queue", str, "Destination queue (or exchange) name; NULL unless running.", true),
}

var shovelsExamples = []vgi.CatalogExample{
	{SQL: "SELECT vhost, name, state, reason FROM rabbitmq.main.shovels() WHERE state <> 'running' ORDER BY vhost, name",
		Description: "Shovels that are not running and why"},
	{SQL: "SELECT state, count(*) AS shovels FROM rabbitmq.main.shovels() GROUP BY state ORDER BY state",
		Description: "Shovels by state"},
}

func newShovels() vgi.TableFunction {
	return newList(&listFunc[vhostArgs]{
		name: "shovels", columns: shovelsColumns,
		meta: meta(docs{Title: "Shovel Status", Category: "config",
			Keywords: "shovels, shovel status, message moving, replication, /api/shovels",
			DocLLM: "One row per shovel from GET /api/shovels[/{vhost}] (needs the rabbitmq_shovel_management plugin): vhost, name, " +
				"type, state (running/blocked/terminated), reason, node, since, and for running shovels src/dest URIs (credentials masked) " +
				"and queues. Named args: vhost (pushed into the path), secret_name. The shovel's configuration is in parameters(component := 'shovel').",
			DocMD: "## shovels(vhost := NULL)\n\nRuntime status of every shovel (needs rabbitmq_shovel_management)."},
			"Runtime status of shovels: state, reason and endpoints (credentials masked)", shovelsColumns, shovelsExamples),
		fetch: func(ctx context.Context, c *conn, a vhostArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, vhostPath("/shovels", a.Vhost), nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				maskS := func(v any) any {
					if s, ok := vStrNE(v).(string); ok {
						return maskString(s)
					}
					return nil
				}
				reason := vStrNE(m["reason"])
				if s, ok := reason.(string); ok {
					reason = maskString(trimQuotes(s))
				}
				srcQ := vStrNE(m["src_queue"])
				if srcQ == nil {
					srcQ = vStrNE(m["src_exchange"])
				}
				dstQ := vStrNE(m["dest_queue"])
				if dstQ == nil {
					dstQ = vStrNE(m["dest_exchange"])
				}
				return []any{vStr(m["vhost"]), vStr(m["name"]), vStrNE(m["type"]), vStrNE(m["state"]), reason, vStrNE(m["node"]),
					vTime(m["timestamp"]), maskS(m["src_uri"]), srcQ, maskS(m["dest_uri"]), dstQ}
			}), false, nil
		},
	})
}

func trimQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
