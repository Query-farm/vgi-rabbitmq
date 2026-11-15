// Message peeking: stream_messages (non-destructive stream reads over AMQP)
// and peek_messages (classic/quorum queues through the management API's
// "get messages" endpoint, opt-in only because it re-queues).
//
// Both return the shared messaging vocabulary (see README "Messaging
// vocabulary"): source columns, sequence, timestamp, key, payload BLOB,
// payload_text, headers MAP, then RabbitMQ-specific AMQP properties.

package rabbitmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	amqp "github.com/rabbitmq/amqp091-go"
)

var messageColumns = []arrow.Field{
	column("vhost", str, "Virtual host of the queue or stream.", false),
	column("queue", str, "Queue or stream the message was read from.", false),
	column("sequence", i64, "Position of the message in its source: the stream offset (stream_messages); NULL for peek_messages, whose queues have no stable position.", true),
	column("timestamp", tstz, "The AMQP timestamp property set by the publisher; NULL when unset.", true),
	column("key", str, "Routing key the message was published with.", true),
	column("exchange", str, "Exchange the message was published to ('' = default exchange).", true),
	column("payload", blob, "Message body bytes (truncated to the truncate argument in peek_messages).", true),
	column("payload_text", str, "The body as text when it is valid UTF-8, else NULL. Parse JSON bodies with payload_text::JSON.", true),
	column("payload_bytes", i64, "Full body size in bytes (before any truncation).", true),
	column("headers", strMap, "AMQP headers (values as text; nested tables as JSON).", false),
	column("redelivered", boolean, "Whether the broker had delivered the message before.", true),
	column("content_type", str, "content_type property, e.g. 'application/json'.", true),
	column("content_encoding", str, "content_encoding property, e.g. 'gzip'.", true),
	column("message_id", str, "message_id property.", true),
	column("correlation_id", str, "correlation_id property.", true),
	column("reply_to", str, "reply_to property.", true),
	column("message_type", str, "type property (application message type).", true),
	column("app_id", str, "app_id property.", true),
	column("user_id", str, "user_id property (validated by the broker when set).", true),
	column("priority", i64, "priority property.", true),
	column("delivery_mode", i64, "delivery_mode property: 1 transient, 2 persistent.", true),
	column("expiration", str, "expiration property (per-message TTL in milliseconds, as text).", true),
}

// renderHeader renders an AMQP table value as text.
func renderHeader(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return x
	case []byte:
		if utf8.Valid(x) {
			return string(x)
		}
		return base64.StdEncoding.EncodeToString(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339)
	case bool:
		return strconv.FormatBool(x)
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(x)
	case amqp.Decimal:
		return fmt.Sprintf("%de-%d", x.Value, x.Scale)
	case amqp.Table:
		return mustJSON(normalizeTable(x))
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = renderHeader(e)
		}
		return mustJSON(out)
	}
	return fmt.Sprint(v)
}

func normalizeTable(t amqp.Table) map[string]string {
	out := make(map[string]string, len(t))
	for k, v := range t {
		out[k] = renderHeader(v)
	}
	return out
}

func payloadCols(body []byte) (any, any) {
	if body == nil {
		body = []byte{}
	}
	var text any
	if utf8.Valid(body) {
		text = string(body)
	}
	return body, text
}

// ---------------------------------------------------------------- stream_messages

type streamArgs struct {
	Vhost         string     `vgi:"pos=0,doc=Virtual host of the stream, e.g. '/'."`
	Stream        string     `vgi:"pos=1,doc=Stream (queue of type stream) to read."`
	StartOffset   string     `vgi:"name=start_offset,default=first,pattern=^(first|last|next|[0-9]+)$,doc=Where to start: 'first' (oldest retained), 'last' (start of the last chunk), 'next' (only new messages) or a numeric stream offset."`
	StartTime     *time.Time `vgi:"name=start_timestamp,doc=Start at the first chunk published at or after this time (overrides start_offset)."`
	MaxMessages   int64      `vgi:"name=max_messages,default=1000,ge=1,le=10000000,doc=Stop after this many messages."`
	BatchSize     int64      `vgi:"name=batch_size,default=500,ge=1,le=10000,doc=Messages read per batch (and AMQP prefetch); one connection per batch."`
	IdleTimeoutMs int64      `vgi:"name=idle_timeout_ms,default=1000,ge=50,le=60000,doc=End the scan when no message arrives for this long (the tail of the stream)."`
	SecretName    *string    `vgi:"name=secret_name,doc=Name of the rabbitmq secret to use when several exist."`
}

func (a streamArgs) secret() string { return deref(a.SecretName) }

// StreamState is the externalized cursor: NextOffset is the next stream
// offset to read (-1 until the first batch has used the start spec).
type StreamState struct {
	ArgsJSON   string
	NextOffset int64
	Remaining  int64
	Done       bool
}

// StreamMessagesFunc reads a stream without consuming it.
type StreamMessagesFunc struct{}

var streamExamples = []vgi.CatalogExample{
	{SQL: "SELECT sequence, key, payload_text::JSON->>'order_id' AS order_id, headers['tenant'] AS tenant FROM rabbitmq.main.stream_messages('shop', 'events') ORDER BY sequence",
		Description: "Read a stream from the beginning without consuming it"},
	{SQL: "SELECT key, count(*) AS messages, sum(payload_bytes) AS bytes FROM rabbitmq.main.stream_messages('shop', 'events', start_offset := '2', max_messages := 100000) GROUP BY key ORDER BY messages DESC, key",
		Description: "Message counts per routing key from offset 2 on"},
}

func (f *StreamMessagesFunc) Name() string { return "stream_messages" }

func (f *StreamMessagesFunc) Metadata() vgi.FunctionMetadata {
	return meta(docs{Title: "Read Stream Messages", Category: "messages",
		Keywords: "stream, peek, read messages, replay, offset, non-destructive, amqp, x-stream-offset, payload",
		DocLLM: "Read messages from a RabbitMQ stream (queue type stream) without consuming them: streams are append-only logs, so " +
			"reading never removes or reorders anything. Uses AMQP 0-9-1 (the secret's AMQP_URL) with x-stream-offset. Positional: vhost, stream. " +
			"Named: start_offset ('first' default, 'last', 'next' or a numeric offset), start_timestamp (`TIMESTAMPTZ`, overrides start_offset), " +
			"max_messages (default 1000), batch_size (default 500), idle_timeout_ms (default 1000: the scan ends when the tail is reached), secret_name. " +
			"Columns: vhost, queue, sequence (stream offset), timestamp, key (routing key), exchange, payload BLOB, payload_text, payload_bytes, " +
			"headers `MAP`, redelivered and AMQP properties. The next offset is the externalized cursor, so long reads resume across batches. " +
			"Refuses non-stream queues (use peek_messages for those).",
		DocMD: "## stream_messages(vhost, stream, start_offset := 'first', start_timestamp := NULL, max_messages := 1000)\n\n" +
			"Non-destructive read of a stream over AMQP from an offset spec; the stream is never consumed."},
		"Read a stream's messages from an offset or time without consuming them (non-destructive)", messageColumns, streamExamples)()
}

func (f *StreamMessagesFunc) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(streamArgs{}) }

func (f *StreamMessagesFunc) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return bindWithSecret[streamArgs](params, arrow.NewSchema(messageColumns, nil))
}

func (f *StreamMessagesFunc) NewState(params *vgi.ProcessParams) (*StreamState, error) {
	var a streamArgs
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if a.Stream == "" {
		return nil, errors.New("stream_messages: the stream name is empty")
	}
	c, err := resolveConn(params.Secrets, a.secret(), false)
	if err != nil {
		return nil, err
	}
	if c.AMQPURL == "" {
		return nil, errors.New("stream_messages reads over AMQP: add AMQP_URL to the rabbitmq secret, e.g. AMQP_URL 'amqp://user:pass@localhost:5672/'")
	}
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return &StreamState{ArgsJSON: string(b), NextOffset: -1, Remaining: a.MaxMessages}, nil
}

// checkIsStream asks the management API (when configured) whether the queue
// is a stream, so a classic or quorum queue is never even subscribed to.
func checkIsStream(ctx context.Context, c *conn, vhost, queue string) error {
	if c.BaseURL == "" {
		return nil // no management API: the delivery check in readStream still guards
	}
	path := "/queues/" + seg(vhost) + "/" + seg(queue)
	v, err := c.getJSON(ctx, path, url.Values{"disable_stats": {"true"}})
	if err != nil {
		return err
	}
	m, _ := v.(map[string]any)
	if t, _ := m["type"].(string); t != "stream" {
		if t == "" {
			t = "classic"
		}
		return fmt.Errorf("stream_messages: %s/%s is a %s queue, not a stream; reading it over AMQP would consume messages. Use peek_messages(..., peek_requeue := true) instead", vhost, queue, t)
	}
	return nil
}

// dialAMQP opens an AMQP connection for the secret, on the given vhost.
func dialAMQP(c *conn, vhost string) (*amqp.Connection, error) {
	u, err := url.Parse(c.AMQPURL)
	if err != nil || (u.Scheme != "amqp" && u.Scheme != "amqps") {
		return nil, errors.New("the rabbitmq secret's AMQP_URL must be an amqp:// or amqps:// URL")
	}
	cfg := amqp.Config{
		Vhost:           vhost,
		Heartbeat:       10 * time.Second,
		Locale:          "en_US",
		TLSClientConfig: c.TLS,
		Properties:      amqp.Table{"connection_name": "vgi-rabbitmq stream reader", "product": "vgi-rabbitmq-worker", "version": Version},
		Dial:            amqp.DefaultDial(15 * time.Second),
	}
	if u.User == nil {
		switch {
		case c.Bearer != "":
			cfg.SASL = []amqp.Authentication{&amqp.PlainAuth{Username: c.Username, Password: c.Bearer}}
		case c.Username != "":
			cfg.SASL = []amqp.Authentication{&amqp.PlainAuth{Username: c.Username, Password: c.Password}}
		}
	}
	conn, err := amqp.DialConfig(c.AMQPURL, cfg)
	if err != nil {
		return nil, fmt.Errorf("stream_messages: connecting to AMQP: %w", maskedErr(err))
	}
	return conn, nil
}

// maskedErr hides credentials an AMQP error might echo.
func maskedErr(err error) error { return errors.New(maskString(err.Error())) }

// streamOffset reads the x-stream-offset header of a stream delivery.
func streamOffset(h amqp.Table) (int64, bool) {
	switch x := h["x-stream-offset"].(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int:
		return int64(x), true
	}
	return 0, false
}

// readStream reads up to want messages starting at spec. It returns the rows
// and the offset after the last one read.
func readStream(ctx context.Context, c *conn, a streamArgs, spec any, minOffset, want int64) ([][]any, int64, error) {
	conn, err := dialAMQP(c, a.Vhost)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return nil, 0, fmt.Errorf("stream_messages: opening a channel: %w", err)
	}
	defer func() { _ = ch.Close() }()
	// Streams require a prefetch; acks on a stream only return flow-control
	// credit and never remove anything.
	if err := ch.Qos(int(min(want, 10000)), 0, false); err != nil {
		return nil, 0, fmt.Errorf("stream_messages: basic.qos: %w", err)
	}
	deliveries, err := ch.ConsumeWithContext(ctx, a.Stream, "", false, false, false, false, amqp.Table{"x-stream-offset": spec})
	if err != nil {
		return nil, 0, fmt.Errorf("stream_messages: subscribing to %s/%s: %w", a.Vhost, a.Stream, err)
	}
	idle := time.Duration(a.IdleTimeoutMs) * time.Millisecond
	timer := time.NewTimer(idle)
	defer timer.Stop()
	rows := [][]any{}
	next := minOffset
	for int64(len(rows)) < want {
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-timer.C:
			return rows, next, nil
		case d, ok := <-deliveries:
			if !ok {
				if len(rows) == 0 {
					return nil, 0, fmt.Errorf("stream_messages: the broker closed the subscription to %s/%s (is it a stream?)", a.Vhost, a.Stream)
				}
				return rows, next, nil
			}
			off, isStream := streamOffset(d.Headers)
			if !isStream {
				// Not a stream: do not ack; closing the channel re-queues it.
				return nil, 0, fmt.Errorf("stream_messages: %s/%s is not a stream (deliveries carry no x-stream-offset); use peek_messages instead", a.Vhost, a.Stream)
			}
			_ = d.Ack(false)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(idle)
			// A subscription starts at a chunk boundary: skip messages
			// before the requested offset.
			if off < minOffset {
				continue
			}
			rows = append(rows, deliveryRow(a.Vhost, a.Stream, &d, off))
			next = off + 1
		}
	}
	return rows, next, nil
}

func deliveryRow(vhost, queue string, d *amqp.Delivery, off int64) []any {
	headers := map[string]string{}
	for k, v := range d.Headers {
		if k == "x-stream-offset" {
			continue
		}
		headers[k] = renderHeader(v)
	}
	body, text := payloadCols(d.Body)
	var ts any
	if !d.Timestamp.IsZero() {
		ts = d.Timestamp.UTC()
	}
	return []any{vhost, queue, off, ts, d.RoutingKey, d.Exchange, body, text, int64(len(d.Body)), headers, d.Redelivered,
		optStr(d.ContentType), optStr(d.ContentEncoding), optStr(d.MessageId), optStr(d.CorrelationId), optStr(d.ReplyTo),
		optStr(d.Type), optStr(d.AppId), optStr(d.UserId), int64(d.Priority), optMode(d.DeliveryMode), optStr(d.Expiration)}
}

func optStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func optMode(m uint8) any {
	if m == 0 {
		return nil
	}
	return int64(m)
}

func (f *StreamMessagesFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *StreamState, out *vgirpc.OutputCollector) error {
	if st.Done || st.Remaining <= 0 {
		return out.Finish()
	}
	var a streamArgs
	if err := json.Unmarshal([]byte(st.ArgsJSON), &a); err != nil {
		return err
	}
	c, err := resolveConn(params.Secrets, a.secret(), false)
	if err != nil {
		return err
	}
	var spec any
	minOffset := int64(0)
	if st.NextOffset >= 0 {
		spec, minOffset = st.NextOffset, st.NextOffset
	} else {
		if err := checkIsStream(ctx, c, a.Vhost, a.Stream); err != nil {
			return err
		}
		switch {
		case a.StartTime != nil:
			spec = a.StartTime.UTC().Truncate(time.Second)
		case a.StartOffset == "first" || a.StartOffset == "last" || a.StartOffset == "next":
			spec = a.StartOffset
		default:
			n, err := strconv.ParseInt(a.StartOffset, 10, 64)
			if err != nil {
				return fmt.Errorf("stream_messages: start_offset must be first, last, next or a number, got %q", a.StartOffset)
			}
			spec, minOffset = n, n
		}
	}
	want := min(a.BatchSize, st.Remaining)
	rows, next, err := readStream(ctx, c, a, spec, minOffset, want)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		st.NextOffset = next
	}
	st.Remaining -= int64(len(rows))
	if int64(len(rows)) < want {
		st.Done = true // reached the tail
	}
	if st.NextOffset < 0 && !st.Done {
		st.Done = true
	}
	return emitRows(out, messageColumns, rows)
}

// ---------------------------------------------------------------- peek_messages

type peekArgs struct {
	Vhost       string  `vgi:"pos=0,doc=Virtual host of the queue."`
	Queue       string  `vgi:"pos=1,doc=Classic or quorum queue to peek."`
	Count       int64   `vgi:"name=count,default=10,ge=1,le=10000,doc=Messages to fetch from the head of the queue."`
	PeekRequeue bool    `vgi:"name=peek_requeue,default=false,doc=Required opt-in: true acknowledges that fetched messages are re-queued, which marks them redelivered and can change their order."`
	Truncate    int64   `vgi:"name=truncate,default=50000,ge=1,le=100000000,doc=Return at most this many bytes of each body (payload_bytes still gives the full size)."`
	SecretName  *string `vgi:"name=secret_name,doc=Name of the rabbitmq secret to use when several exist."`
}

func (a peekArgs) secret() string { return deref(a.SecretName) }

var peekExamples = []vgi.CatalogExample{
	{SQL: "SELECT message_id, key, payload_text, headers['trace'] AS trace FROM rabbitmq.main.peek_messages('shop', 'orders.created', count := 5, peek_requeue := true)",
		Description: "Look at the first five messages of a classic queue (re-queues them: opt-in)"},
	{SQL: "SELECT content_type, count(*) AS messages, max(payload_bytes) AS largest FROM rabbitmq.main.peek_messages('shop', 'orders.dlq', count := 100, peek_requeue := true) GROUP BY content_type ORDER BY messages DESC",
		Description: "What kinds of messages sit in a dead-letter queue"},
}

const peekWarning = "WARNING: classic and quorum queues have no read-only peek. peek_messages fetches with ackmode " +
	"reject_requeue_true, so every fetched message is re-queued: it is marked redelivered, a quorum queue counts a delivery " +
	"attempt against its delivery-limit (a message can be dead-lettered or dropped), and with active consumers the order can change. "

func newPeekMessages() vgi.TableFunction {
	return newList(&listFunc[peekArgs]{
		name: "peek_messages", columns: messageColumns,
		validate: func(a peekArgs) error {
			if !a.PeekRequeue {
				return errInvalid("peek_messages re-queues the messages it fetches (marking them redelivered, counting a delivery against a quorum queue's delivery-limit, and possibly reordering them); pass peek_requeue := true to accept that, or read a stream non-destructively with stream_messages")
			}
			return nil
		},
		meta: meta(docs{Title: "Peek Queue Messages (Re-queues)", Category: "messages",
			Keywords: "peek, get messages, inspect queue, dead letter queue, requeue, debug, payload",
			DocLLM: peekWarning + "Use stream_messages for streams (truly non-destructive). " +
				"Fetches up to count messages from the head of a classic or quorum queue with POST /api/queues/{vhost}/{queue}/get. " +
				"Positional: vhost, queue. Named: count (default 10), peek_requeue (must be true, default false = error), truncate " +
				"(body bytes, default 50000), secret_name. Columns as stream_messages; sequence is NULL.",
			DocMD: "## peek_messages(vhost, queue, count := 10, peek_requeue := false)\n\n**" + peekWarning + "** " +
				"It refuses to run unless you pass `peek_requeue := true`. Prefer `stream_messages` for streams."},
			"Fetch and re-queue messages from a classic or quorum queue (opt-in: peek_requeue := true)", messageColumns, peekExamples),
		fetch: func(ctx context.Context, c *conn, a peekArgs, _ int64) ([][]any, bool, error) {
			path := "/queues/" + seg(a.Vhost) + "/" + seg(a.Queue) + "/get"
			status, body, err := c.call(ctx, path, nil, reqOpts{method: http.MethodPost, body: map[string]any{
				"count": a.Count, "ackmode": "reject_requeue_true", "encoding": "auto", "truncate": a.Truncate}})
			if err != nil {
				return nil, false, err
			}
			if status/100 != 2 {
				return nil, false, errorFrom(path, status, body)
			}
			v, err := decodeJSON(path, body)
			if err != nil {
				return nil, false, err
			}
			items, err := asObjects(path, v)
			if err != nil {
				return nil, false, err
			}
			rows := make([][]any, 0, len(items))
			for _, m := range items {
				row, err := peekRow(a, m)
				if err != nil {
					return nil, false, err
				}
				rows = append(rows, row)
			}
			return rows, false, nil
		},
	})
}

func peekRow(a peekArgs, m map[string]any) ([]any, error) {
	payload := []byte{}
	if s, ok := m["payload"].(string); ok {
		if enc, _ := m["payload_encoding"].(string); enc == "base64" {
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, fmt.Errorf("peek_messages: decoding a base64 payload: %w", err)
			}
			payload = b
		} else {
			payload = []byte(s)
		}
	}
	body, text := payloadCols(payload)
	props, _ := m["properties"].(map[string]any)
	if props == nil {
		props = map[string]any{}
	}
	headers := map[string]string{}
	if h, ok := props["headers"].(map[string]any); ok {
		for k, v := range h {
			headers[k] = renderValue(v)
		}
	}
	size := vInt(m["payload_bytes"])
	if size == nil {
		size = int64(len(payload))
	}
	return []any{a.Vhost, a.Queue, nil, vEpochS(props["timestamp"]), vStr(m["routing_key"]), vStr(m["exchange"]),
		body, text, size, headers, vBool(m["redelivered"]),
		vStrNE(props["content_type"]), vStrNE(props["content_encoding"]), vStrNE(props["message_id"]),
		vStrNE(props["correlation_id"]), vStrNE(props["reply_to"]), vStrNE(props["type"]), vStrNE(props["app_id"]),
		vStrNE(props["user_id"]), vInt(props["priority"]), vInt(props["delivery_mode"]), vStrNE(props["expiration"])}, nil
}

