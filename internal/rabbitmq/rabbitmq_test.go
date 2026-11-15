package rabbitmq

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestMaskString(t *testing.T) {
	cases := map[string]string{
		"amqp://user:pass@host:5672/vh":            "amqp://user:****@host:5672/vh",
		"amqps://u:p%40ss@h/%2f?cacertfile=/x.pem": "amqps://u:****@h/%2f?cacertfile=/x.pem",
		"amqp://host":                           "amqp://host",
		"amqp://user@host":                      "amqp://user@host",
		"amqp://h/?heartbeat=5&password=pw&x=1": "amqp://h/?heartbeat=5&password=****&x=1",
		"amqp://h/?keyfile=/k.pem":              "amqp://h/?keyfile=/k.pem",
		"not a uri: a:b@c":                      "not a uri: a:b@c",
	}
	for in, want := range cases {
		if got := maskString(in); got != want {
			t.Errorf("maskString(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMaskValueNested(t *testing.T) {
	v := map[string]any{
		"src-uri":       []any{"amqp://a:pw1@h1", "amqp://h2"},
		"dest-password": "pw2",
		"nested":        map[string]any{"token": "t", "uri": "amqp://b:pw3@h"},
		"count":         json.Number("3"),
	}
	b, _ := json.Marshal(maskValue(v))
	s := string(b)
	for _, secret := range []string{"pw1", "pw2", "pw3", `"t"`} {
		if strings.Contains(s, secret) {
			t.Fatalf("masked value still contains %s: %s", secret, s)
		}
	}
	if !strings.Contains(s, `"count":3`) || !strings.Contains(s, "amqp://h2") {
		t.Fatalf("masking changed non-secret values: %s", s)
	}
}

func TestValueHelpers(t *testing.T) {
	m := map[string]any{
		"n":     json.Number("9007199254740993"),
		"f":     json.Number("1.5"),
		"tags":  "administrator, monitoring",
		"list":  []any{"a", json.Number("2")},
		"args":  map[string]any{"x-queue-type": "quorum", "x-delivery-limit": json.Number("5"), "x-null": nil, "x-obj": map[string]any{"k": true}},
		"since": "2026-11-15 14:17:05",
		"rfc":   "2026-11-15T14:17:05.123+00:00",
		"stats": map[string]any{"publish_details": map[string]any{"rate": json.Number("2.5")}},
	}
	if vInt(m["n"]) != int64(9007199254740993) {
		t.Errorf("vInt lost precision: %v", vInt(m["n"]))
	}
	if vFloat(m["f"]) != 1.5 || vInt(m["missing"]) != nil || vStrNE("") != nil {
		t.Error("scalar helpers")
	}
	if l := vList(m["tags"]).([]string); len(l) != 2 || l[1] != "monitoring" {
		t.Errorf("comma tags: %v", l)
	}
	if l := vList(m["list"]).([]string); l[1] != "2" {
		t.Errorf("list: %v", l)
	}
	args := vMap(m["args"]).(map[string]string)
	if args["x-delivery-limit"] != "5" || args["x-obj"] != `{"k":true}` {
		t.Errorf("map: %v", args)
	}
	if _, ok := args["x-null"]; ok {
		t.Error("null map values are dropped")
	}
	if ts := vTime(m["since"]).(time.Time); ts.Hour() != 14 {
		t.Errorf("time: %v", ts)
	}
	if ts := vTime(m["rfc"]).(time.Time); ts.Nanosecond() != 123000000 {
		t.Errorf("rfc time: %v", ts)
	}
	if rate(m, "stats", "publish") != 2.5 || rate(m, "stats", "ack") != nil {
		t.Error("rate")
	}
	if vEpochS(json.Number("0")) != nil || vEpochMs(json.Number("1767225600000")).(time.Time).Year() != 2026 {
		t.Error("epochs")
	}
}

func TestRetryDelay(t *testing.T) {
	if d := retryDelay("2", 1); d != 2*time.Second {
		t.Errorf("seconds: %v", d)
	}
	if d := retryDelay("999", 1); d != 30*time.Second {
		t.Errorf("capped: %v", d)
	}
	if d := retryDelay("", 2); d != time.Second {
		t.Errorf("backoff: %v", d)
	}
}

func noSleep(t *testing.T) {
	old := sleep
	sleep = func(context.Context, time.Duration) {}
	t.Cleanup(func() { sleep = old })
}

func testConn(url string) *conn {
	return &conn{BaseURL: url, Headers: http.Header{"Authorization": {"Basic x"}}, client: defaultClient}
}

func TestCallRetries429And503(t *testing.T) {
	noSleep(t)
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch n.Add(1) {
		case 1:
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(429)
		case 2:
			w.WriteHeader(503)
		default:
			if r.URL.EscapedPath() != "/api/queues/%2F" {
				t.Errorf("vhost not escaped: %s", r.URL.EscapedPath())
			}
			_, _ = w.Write([]byte(`[{"name":"q"}]`))
		}
	}))
	defer srv.Close()
	items, err := testConn(srv.URL).getList(context.Background(), vhostPath("/queues", ptr("/")), nil)
	if err != nil || len(items) != 1 || n.Load() != 3 {
		t.Fatalf("items=%v err=%v calls=%d", items, err, n.Load())
	}
}

func TestHealthCheck503IsAnAnswer(t *testing.T) {
	noSleep(t)
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(503)
		_, _ = w.Write([]byte(`{"status":"failed","reason":"alarms"}`))
	}))
	defer srv.Close()
	status, _, err := testConn(srv.URL).call(context.Background(), "/health/checks/alarms", nil, reqOpts{healthCheck: true})
	if err != nil || status != 503 || n.Load() != 1 {
		t.Fatalf("status=%d err=%v calls=%d (a failed check must not be retried)", status, err, n.Load())
	}
}

func TestErrorsNameStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"error":"not_authorized","reason":"Login failed"}`))
	}))
	defer srv.Close()
	_, err := testConn(srv.URL).getJSON(context.Background(), "/overview", nil)
	if err == nil || !strings.Contains(err.Error(), "HTTP 401") || !strings.Contains(err.Error(), "Login failed") {
		t.Fatalf("err = %v", err)
	}
}

func TestGetPage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("name") != "^o" || q.Get("use_regex") != "true" || q.Get("page_size") != "2" {
			t.Errorf("query: %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"items":[{"name":"a"},{"name":"b"}],"page":` + q.Get("page") + `,"page_count":3}`))
	}))
	defer srv.Close()
	a := pagedArgs{Name: ptr("^o"), UseRegex: true, PageSize: 2}
	rows, more, err := fetchPaged(context.Background(), testConn(srv.URL), "/queues", a, 2, func(m map[string]any) []any { return []any{m["name"]} })
	if err != nil || len(rows) != 2 || !more {
		t.Fatalf("rows=%v more=%v err=%v", rows, more, err)
	}
	_, more, _ = fetchPaged(context.Background(), testConn(srv.URL), "/queues", a, 3, func(m map[string]any) []any { return nil })
	if more {
		t.Error("the last page must end the scan")
	}
}

func TestResolveConn(t *testing.T) {
	secrets := fakeSecrets(map[string]string{"management_url": "https://rabbit.example.com/api/", "bearer_token": "tok"})
	c, err := resolveConn(secrets, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if c.BaseURL != "https://rabbit.example.com" || c.Headers.Get("Authorization") != "Bearer tok" {
		t.Fatalf("conn %+v", c)
	}
	if _, err := resolveConn(fakeSecrets(map[string]string{"management_url": "ftp://x"}), "", true); err == nil {
		t.Error("non-http URL accepted")
	}
	if _, err := resolveConn(fakeSecrets(map[string]string{"management_url": "http://x", "tls_skip_verify": "maybe"}), "", true); err == nil {
		t.Error("bad tls_skip_verify accepted")
	}
	c, err = resolveConn(fakeSecrets(map[string]string{"amqp_url": "amqp://h"}), "", false)
	if err != nil || c.AMQPURL != "amqp://h" {
		t.Fatalf("amqp-only secret: %v", err)
	}
}

func TestPeekRow(t *testing.T) {
	m := map[string]any{"payload": "AP/+AQ==", "payload_encoding": "base64", "payload_bytes": json.Number("4"),
		"routing_key": "k", "exchange": "", "redelivered": true,
		"properties": map[string]any{"timestamp": json.Number("1767225600"), "headers": map[string]any{"n": json.Number("1")}, "priority": json.Number("3")}}
	row, err := peekRow(peekArgs{Vhost: "/", Queue: "q"}, m)
	if err != nil {
		t.Fatal(err)
	}
	if b := row[6].([]byte); len(b) != 4 || b[1] != 0xff {
		t.Errorf("payload %v", row[6])
	}
	if row[7] != nil {
		t.Error("binary payload has no text")
	}
	if row[9].(map[string]string)["n"] != "1" || row[3].(time.Time).Year() != 2026 || row[19] != int64(3) {
		t.Errorf("row %v", row)
	}
	if len(row) != len(messageColumns) {
		t.Fatalf("row has %d values for %d columns", len(row), len(messageColumns))
	}
}

func TestDeliveryRow(t *testing.T) {
	d := &amqp.Delivery{Headers: amqp.Table{"x-stream-offset": int64(7), "tenant": "acme", "n": int32(2), "t": amqp.Table{"a": "b"}},
		Body: []byte(`{"a":1}`), RoutingKey: "k", Exchange: "x", DeliveryMode: 2, Timestamp: time.Unix(1767225600, 0)}
	off, ok := streamOffset(d.Headers)
	if !ok || off != 7 {
		t.Fatal("stream offset")
	}
	row := deliveryRow("/", "s", d, off)
	h := row[9].(map[string]string)
	if _, has := h["x-stream-offset"]; has || h["n"] != "2" || h["t"] != `{"a":"b"}` {
		t.Errorf("headers %v", h)
	}
	if row[7] != `{"a":1}` || row[2] != int64(7) || row[20] != int64(2) || len(row) != len(messageColumns) {
		t.Errorf("row %v", row)
	}
	if _, ok := streamOffset(amqp.Table{}); ok {
		t.Error("a classic delivery has no stream offset")
	}
}

func TestWorkerBuilds(t *testing.T) {
	_ = NewWorker()
	for _, ex := range allExamples() {
		if !strings.Contains(ex.SQL, "rabbitmq.main.") {
			t.Errorf("example is not fully qualified: %s", ex.SQL)
		}
	}
}

func ptr(s string) *string { return &s }

// fakeSecrets builds the secrets the SDK resolves for one unnamed rabbitmq secret.
func fakeSecrets(fields map[string]string) vgi.Secrets {
	m := map[string]interface{}{"type": SecretType}
	for k, v := range fields {
		m[k] = v
	}
	return vgi.Secrets{"__default_" + SecretType: m}
}
