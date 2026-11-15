// Discovery metadata: the one file to edit when you rename or extend the worker.
//
// DuckDB, coding agents and the vgi-lint metadata checker all read these tags.
// Keep every object documented and `make lint-explain` stays at 100.
//
//   - Catalog-level tags (title, license, author, links, long description)
//     live in CatalogTags below.
//   - Every function carries objectTags(...): a title, a category from
//     SchemaCategories, search keywords, and LLM- and human-facing docs.

package rabbitmq

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogName is the name DuckDB attaches the worker under.
const CatalogName = "rabbitmq"

// SourceURL is where this worker's code lives.
const SourceURL = "https://github.com/Query-farm/vgi-rabbitmq"

// category is one entry of the schema's vgi.categories list.
type category struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// SchemaCategories group functions in docs and agent tools. Every function's
// vgi.category must name one of these.
var SchemaCategories = []category{
	{Name: "broker", Title: "Broker And Cluster", Description: "Cluster overview, nodes with resource alarms and partitions, and virtual hosts."},
	{Name: "topology", Title: "Topology", Description: "Queues, exchanges and the bindings that route between them."},
	{Name: "clients", Title: "Clients", Description: "Connections, channels and consumers."},
	{Name: "access", Title: "Access Control", Description: "Users, vhost permissions and topic permissions (no password hashes)."},
	{Name: "config", Title: "Configuration", Description: "Policies, operator policies, runtime and global parameters, shovels and the definitions export."},
	{Name: "health", Title: "Health", Description: "Health checks, feature flags and deprecated features in use."},
	{Name: "messages", Title: "Messages", Description: "Non-destructive stream reads and opt-in re-queueing peeks at queue messages."},
}

// mustJSON marshals v, panicking on failure (only static data is passed).
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// keywordsArray turns "a, b, c" into the JSON array string vgi.keywords requires.
func keywordsArray(keywords string) string {
	out := []string{}
	for _, k := range strings.Split(keywords, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return mustJSON(out)
}

// docs is the set of standard per-object discovery fields.
type docs struct {
	Title    string
	Category string
	Keywords string
	DocLLM   string
	DocMD    string
}

// objectTags builds the standard per-object discovery tags.
func objectTags(d docs) map[string]string {
	known := false
	for _, c := range SchemaCategories {
		known = known || c.Name == d.Category
	}
	if !known {
		panic(fmt.Sprintf("unknown category %q; add it to SchemaCategories", d.Category))
	}
	return map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": d.Category,
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}
}

// resultColumn describes one output column for vgi.result_columns_schema.
type resultColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// column builds an Arrow field whose comment shows up in DuckDB's DESCRIBE and agent tools.
func column(name string, typ arrow.DataType, comment string, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: typ, Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{"comment"}, []string{comment})}
}

// exampleQuery is one entry of vgi.example_queries.
type exampleQuery struct {
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

// exampleQueries converts catalog examples to vgi.example_queries entries.
func exampleQueries(lists ...[]vgi.CatalogExample) []exampleQuery {
	out := []exampleQuery{}
	for _, examples := range lists {
		for _, ex := range examples {
			out = append(out, exampleQuery{Description: ex.Description, SQL: ex.SQL})
		}
	}
	return out
}

// withExamples publishes examples (with descriptions) as vgi.example_queries.
// Overloads share one SQL name, so pass every overload's examples.
func withExamples(tags map[string]string, lists ...[]vgi.CatalogExample) map[string]string {
	tags["vgi.example_queries"] = mustJSON(exampleQueries(lists...))
	return tags
}

const catalogDocLLM = "Inspect a RabbitMQ broker from SQL through its Management HTTP API, and read messages without consuming them. " +
	"Every function needs a DuckDB secret: CREATE SECRET (TYPE rabbitmq, MANAGEMENT_URL 'http://host:15672', USERNAME/PASSWORD or " +
	"BEARER_TOKEN for OAuth 2, AMQP_URL for stream_messages, optional TLS_CA_CERT/TLS_CLIENT_CERT/TLS_CLIENT_KEY/TLS_SKIP_VERIFY). " +
	"Broker: overview, nodes (memory/disk alarms, partitions), vhosts. Topology: queues (backlog = messages_ready, in flight = " +
	"messages_unacknowledged, type classic/quorum/stream), exchanges, bindings (routing graph). Clients: connections, channels, consumers. " +
	"Access: users (never password hashes), permissions, topic_permissions. Config: policies, operator_policies, parameters and " +
	"global_parameters (credentials in URIs masked), shovels, definitions_summary. Health: health_checks, feature_flags, deprecated_features. " +
	"Messages: stream_messages(vhost, stream) reads a stream from an offset spec without consuming; peek_messages(vhost, queue, " +
	"peek_requeue := true) fetches from a classic/quorum queue and re-queues (marks redelivered), so it refuses to run without the opt-in. " +
	"vhost := and name := (substring, or regex with use_regex := true) are pushed into the API; listings page with an externalized cursor."

// CatalogDocMD is the catalog's comment: one short paragraph for directories.
const CatalogDocMD = "RabbitMQ broker state as SQL tables through the Management HTTP API (queues, exchanges, bindings, " +
	"connections, nodes, policies, permissions, health checks) plus non-destructive stream reads."

const catalogDocMDLong = "# RabbitMQ in DuckDB\n\n" +
	"Query a RabbitMQ cluster's state with SQL and join it with anything else DuckDB reads.\n\n" +
	"- **Broker**: overview, nodes with memory/disk alarms and partitions, virtual hosts.\n" +
	"- **Topology**: queues (depth, consumers, rates, type, policy, arguments), exchanges, bindings.\n" +
	"- **Clients**: connections, channels, consumers.\n" +
	"- **Access**: users (no password hashes), permissions, topic permissions.\n" +
	"- **Config**: policies, operator policies, runtime parameters with masked credentials, shovels, definitions summary.\n" +
	"- **Health**: health checks, feature flags, deprecated features in use.\n" +
	"- **Messages**: `stream_messages` reads streams without consuming them; `peek_messages` peeks classic/quorum " +
	"queues only with `peek_requeue := true`, because it re-queues what it fetches.\n\n" +
	"Credentials come from a `rabbitmq` DuckDB secret. Everything is read-only except the opt-in re-queueing peek."

const schemaDocLLM = "The main schema holds every function; all are table functions. Each takes an optional secret_name named argument " +
	"(default: the rabbitmq secret whose scope matches). Most listings take vhost := (pushed into the API path); queues, exchanges, " +
	"connections and channels also take name := (case-insensitive substring, or a regex with use_regex := true) and page_size := " +
	"(max 500). Arguments, policy definitions and headers are MAP(VARCHAR, VARCHAR) with values as text: read one with " +
	"arguments['x-queue-type']. Times are TIMESTAMPTZ, counters BIGINT, rates DOUBLE per second. stream_messages and peek_messages " +
	"share the messaging columns sequence, timestamp, key, payload (BLOB), payload_text, headers."

// SchemaDocMD is the main schema's human-facing description (also its comment).
const SchemaDocMD = "## main\n\n" +
	"Every function the RabbitMQ worker exposes. Management functions call the Management HTTP API named by a " +
	"`rabbitmq` secret with read-only GETs; `stream_messages` reads streams over AMQP without consuming them.\n\n" +
	"Start with `overview()` and `queues()`; use `bindings()` to follow routes and `health_checks()` for alarms."

// agentTask is one public analyst task for `vgi-lint simulate`.
type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

// agentTestTasks are graded privately by vgi-agent-tests.yaml. They assume a
// rabbitmq secret pointing at the fixture broker (test/fixture).
var agentTestTasks = []agentTask{
	{Name: "backlog", Prompt: "Which queues in the shop vhost have messages ready, and how many each?"},
	{Name: "queue_types", Prompt: "How many queues of each type (classic, quorum, stream) does the broker have?"},
	{Name: "routes_from_orders", Prompt: "Which queues and exchanges does the 'orders' exchange in vhost 'shop' route to, and with which routing keys?"},
	{Name: "admins", Prompt: "Which RabbitMQ users are administrators?"},
	{Name: "shovel_destination", Prompt: "Where does the shovel 'orders-to-archive' send messages? Give its destination URI."},
	{Name: "policy_ttl", Prompt: "What message TTL does the policy orders-ttl set, and which queues does it apply to?"},
	{Name: "node_alarms", Prompt: "Is any RabbitMQ node under a memory or disk alarm?"},
	{Name: "stream_keys", Prompt: "How many messages per routing key are in the 'events' stream of vhost 'shop'? Do not consume anything."},
	{Name: "server_version", Prompt: "Which RabbitMQ version is the broker running?"},
}

// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "VGI RabbitMQ Worker",
	"vgi.subtitle":           "RabbitMQ broker state via the Management API, plus non-destructive stream reads.",
	"vgi.doc_llm":            catalogDocLLM,
	"vgi.doc_md":             catalogDocMDLong,
	"vgi.source_url":         SourceURL,
	"vgi.icon_url":           "https://query.farm/media-kit/products/vgi/vgi-logo.png",
	"vgi.license":            "0BSD",
	"vgi.author":             "Query.Farm",
	"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
	"vgi.keywords":           keywordsArray("rabbitmq, amqp, message broker, queues, streams, management api, monitoring, messaging, quorum queues"),
	"vgi.support_contact":    SourceURL + "/issues",
	"vgi.support_policy_url": SourceURL + "#support",
	// Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
	"vgi.agent_test_tasks": mustJSON(agentTestTasks),
}

// schemaTags builds the main schema's tags; allExamples feeds vgi.example_queries.
func schemaTags(allExamples []exampleQuery) map[string]string {
	return map[string]string{
		"vgi.title":           "RabbitMQ Functions",
		"domain":              "messaging",
		"vgi.keywords":        keywordsArray("queues, exchanges, bindings, connections, nodes, policies, health checks, streams, peek"),
		"vgi.doc_llm":         schemaDocLLM,
		"vgi.doc_md":          SchemaDocMD,
		"vgi.categories":      mustJSON(SchemaCategories),
		"vgi.example_queries": mustJSON(allExamples),
	}
}
