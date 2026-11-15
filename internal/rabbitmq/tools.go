// Offline tools: scalars that need no broker and no secret. They carry the
// worker's executable examples.
//
//	SELECT rabbitmq.main.topic_matches('orders.*.eu', 'orders.created.eu'); -- true
//	SELECT rabbitmq.main.mask_uri('amqp://u:p@h/');                         -- amqp://u:****@h/

package rabbitmq

import (
	"context"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// executableExample is one vgi.executable_examples entry.
type executableExample struct {
	Description string  `json:"description"`
	SQL         string  `json:"sql"`
	Expected    [][]any `json:"expected_result"`
}

// topicMatches reports whether an AMQP topic binding key matches a routing
// key: words are '.'-separated, '*' matches exactly one word and '#' zero or
// more words.
func topicMatches(pattern, key string) bool {
	p := strings.Split(pattern, ".")
	k := strings.Split(key, ".")
	if key == "" {
		k = []string{}
	}
	if pattern == "" {
		p = []string{}
	}
	// memo[i][j]: p[i:] matches k[j:]
	memo := make(map[[2]int]bool)
	seen := make(map[[2]int]bool)
	var match func(i, j int) bool
	match = func(i, j int) bool {
		idx := [2]int{i, j}
		if seen[idx] {
			return memo[idx]
		}
		var r bool
		switch {
		case i == len(p):
			r = j == len(k)
		case p[i] == "#":
			r = match(i+1, j) || (j < len(k) && match(i, j+1))
		case j == len(k):
			r = false
		case p[i] == "*" || p[i] == k[j]:
			r = match(i+1, j+1)
		}
		seen[idx], memo[idx] = true, r
		return r
	}
	return match(0, 0)
}

// ---------------------------------------------------------------- topic_matches

type topicArgs struct {
	Pattern    *array.String `vgi:"pos=0,const=false,doc=Topic binding key, e.g. 'orders.*.eu' or 'orders.#'. NULL gives NULL."`
	RoutingKey *array.String `vgi:"pos=1,const=false,doc=Routing key of a message, e.g. 'orders.created.eu'. NULL gives NULL."`
}

// TopicMatches is topic_matches(pattern, routing_key).
type TopicMatches struct{}

var topicExamples = []vgi.CatalogExample{
	{SQL: "SELECT destination FROM rabbitmq.main.bindings(vhost := 'shop') WHERE source = 'orders' AND rabbitmq.main.topic_matches(routing_key, 'orders.created.eu') ORDER BY destination",
		Description: "Which destinations a message published to the orders topic exchange with key orders.created.eu reaches"},
}

var topicExecutable = []executableExample{{Description: "Topic wildcards: * is one word, # is zero or more",
	SQL:      "SELECT p, rabbitmq.main.topic_matches(p, 'orders.created.eu') AS hit FROM (VALUES ('orders.*'), ('orders.*.eu'), ('orders.#'), ('#'), ('*.paid')) t(p) ORDER BY p",
	Expected: [][]any{{"#", true}, {"*.paid", false}, {"orders.#", true}, {"orders.*", false}, {"orders.*.eu", true}}}}

// Name is the SQL name.
func (f *TopicMatches) Name() string { return "topic_matches" }

// Metadata describes the function.
func (f *TopicMatches) Metadata() vgi.FunctionMetadata {
	tags := withExamples(objectTags(docs{Title: "Topic Binding Match", Category: "tools",
		Keywords: "topic exchange, binding key, routing key, wildcard, routing simulation",
		DocLLM: "topic_matches(pattern, routing_key) returns true when an AMQP topic-exchange binding key matches a routing key, " +
			"with RabbitMQ semantics: words are separated by '.', '*' matches exactly one word, '#' matches zero or more words. " +
			"NULL in, NULL out. Join it with bindings() to see where a message would be routed. Runs locally: no broker, no secret.",
		DocMD: "## topic_matches(pattern, routing_key)\n\n`true` when a topic binding key matches a routing key (`*` = one word, `#` = zero or more)."}),
		topicExamples)
	tags["vgi.executable_examples"] = mustJSON(topicExecutable)
	return vgi.FunctionMetadata{Description: "True when an AMQP topic binding key (with * and # wildcards) matches a routing key",
		Stability: vgi.StabilityConsistent, Tags: tags, Examples: topicExamples}
}

// OnBindTyped declares a BOOLEAN result.
func (f *TopicMatches) OnBindTyped(*topicArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.FixedWidthTypes.Boolean)
}

// ProcessTyped evaluates one batch.
func (f *TopicMatches) ProcessTyped(_ context.Context, _ *topicArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	pc, kc := batch.Column(0), batch.Column(1)
	b := array.NewBooleanBuilder(memory.NewGoAllocator())
	defer b.Release()
	for i := 0; i < int(batch.NumRows()); i++ {
		if pc.IsNull(i) || kc.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(topicMatches(vgi.GetStringValue(pc, i), vgi.GetStringValue(kc, i)))
	}
	return resultBatch(b.NewArray(), batch.NumRows()), nil
}

func resultBatch(arr arrow.Array, rows int64) arrow.RecordBatch {
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arr.DataType(), Nullable: true}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, rows)
}

// ---------------------------------------------------------------- mask_uri

type maskArgs struct {
	URI *array.String `vgi:"pos=0,const=false,doc=Text that may contain URIs with credentials, e.g. 'amqp://user:pass@host/vhost'. NULL gives NULL."`
}

// MaskURI is mask_uri(text).
type MaskURI struct{}

var maskExamples = []vgi.CatalogExample{
	{SQL: "SELECT name, rabbitmq.main.mask_uri(json_extract_string(value, '$.\"src-uri\"')) AS src FROM rabbitmq.main.parameters(component := 'shovel') ORDER BY name",
		Description: "Shovel source URIs with credentials masked (parameters already masks; this works on any text)"},
}

var maskExecutable = []executableExample{{Description: "Mask passwords in URIs and query strings",
	SQL:      "SELECT rabbitmq.main.mask_uri(u) AS masked FROM (VALUES ('amqp://app:s3cret@mq:5672/shop'), ('amqps://mq/?password=x&heartbeat=5'), ('amqp://mq')) t(u) ORDER BY masked",
	Expected: [][]any{{"amqp://app:****@mq:5672/shop"}, {"amqp://mq"}, {"amqps://mq/?password=****&heartbeat=5"}}}}

// Name is the SQL name.
func (f *MaskURI) Name() string { return "mask_uri" }

// Metadata describes the function.
func (f *MaskURI) Metadata() vgi.FunctionMetadata {
	tags := withExamples(objectTags(docs{Title: "Mask URI Credentials", Category: "tools",
		Keywords: "mask, redact, credentials, password, amqp uri, connection string",
		DocLLM: "mask_uri(text) replaces the password in every scheme://user:password@host URI with '****' and the value of " +
			"password/token/secret query parameters, leaving the rest intact (the same masking parameters() applies). NULL in, NULL out. " +
			"Use it before sharing connection strings from logs or config tables. Runs locally: no broker, no secret.",
		DocMD: "## mask_uri(text)\n\nMasks passwords in URIs (`amqp://user:****@host`) and password-like query parameters."}),
		maskExamples)
	tags["vgi.executable_examples"] = mustJSON(maskExecutable)
	return vgi.FunctionMetadata{Description: "Mask passwords in AMQP (or any) URIs and password-like query parameters",
		Stability: vgi.StabilityConsistent, Tags: tags, Examples: maskExamples}
}

// OnBindTyped declares a VARCHAR result.
func (f *MaskURI) OnBindTyped(*maskArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

// ProcessTyped evaluates one batch.
func (f *MaskURI) ProcessTyped(_ context.Context, _ *maskArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	for i := 0; i < int(batch.NumRows()); i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(maskString(vgi.GetStringValue(col, i)))
	}
	return resultBatch(b.NewArray(), batch.NumRows()), nil
}
