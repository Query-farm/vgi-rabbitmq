// HTTP client for the RabbitMQ Management API, and the `rabbitmq` secret.
//
// Every management function sends read-only GETs to <management_url>/api/...
// The one exception is peek_messages, which POSTs /api/queues/{vhost}/{q}/get
// with ackmode=reject_requeue_true, and only when the caller opts in with
// peek_requeue := true (see messages.go).
//
// Credentials come from a DuckDB secret of type `rabbitmq`: basic auth
// (username/password) or an OAuth 2 bearer token, plus TLS options shared by
// HTTPS and AMQPS. They are read from the secret on every call and never kept
// in table-function state, which travels back to DuckDB over HTTP.

package rabbitmq

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

const (
	// SecretType is the DuckDB secret type every function reads.
	SecretType = "rabbitmq"
	// MaxAttempts bounds retries of 429/502/503/504 responses and network errors.
	MaxAttempts = 5
	// Timeout bounds every request.
	Timeout = 60 * time.Second
)

// userAgent identifies the worker to the API.
const userAgent = "vgi-rabbitmq-worker/" + Version

func secretField(name, doc string, redact bool) arrow.Field {
	keys := []string{"vgi_doc"}
	vals := []string{doc}
	if redact {
		keys = append(keys, "redact")
		vals = append(vals, "true")
	}
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true,
		Metadata: arrow.NewMetadata(keys, vals)}
}

// APISecretType is registered with DuckDB so users can run
// CREATE SECRET (TYPE rabbitmq, MANAGEMENT_URL '...', USERNAME '...', PASSWORD '...').
var APISecretType = vgi.SecretTypeSpec{
	Name:        SecretType,
	Description: "Connection to a RabbitMQ broker: the Management HTTP API (basic auth or OAuth 2 bearer token), an optional AMQP URL for stream reads, and TLS options.",
	Schema: arrow.NewSchema([]arrow.Field{
		secretField("management_url", "Management plugin root, e.g. 'http://localhost:15672' or 'https://rabbit.example.com/rabbitmq' behind a path prefix. /api/... is appended.", false),
		secretField("username", "Basic-auth user name for the Management API (and for AMQP when amqp_url has no user).", false),
		secretField("password", "Basic-auth password.", true),
		secretField("bearer_token", "OAuth 2 access token, sent as 'Authorization: Bearer <token>' (rabbitmq_auth_backend_oauth2); for AMQP it is sent as the password.", true),
		secretField("amqp_url", "AMQP 0-9-1 URL used by stream_messages, e.g. 'amqp://user:pass@localhost:5672/' or 'amqps://...'. The vhost argument overrides its path.", true),
		secretField("tls_ca_cert", "PEM CA bundle that signs the broker's certificate (HTTPS and AMQPS).", false),
		secretField("tls_client_cert", "PEM client certificate for mutual TLS.", false),
		secretField("tls_client_key", "PEM private key of tls_client_cert.", true),
		secretField("tls_server_name", "Server name to verify instead of the URL's host.", false),
		secretField("tls_skip_verify", "'true' disables certificate verification (testing only).", false),
	}, &secretSchemaMetadata),
}

// secretSchemaMetadata links the secret type to its documentation.
var secretSchemaMetadata = arrow.NewMetadata(
	[]string{"vgi_doc_url"},
	[]string{SourceURL + "#secrets-and-configuration"})

// secretRequirements is declared by every function: resolve the session's
// rabbitmq secret(s) and pass them to the worker.
var secretRequirements = []vgi.SecretRequirement{{SecretType: SecretType}}

// secretNamed is implemented by every argument struct.
type secretNamed interface{ secret() string }

// bindWithSecret binds a table function's schema. When secret_name names a
// secret, the first bind asks the extension to resolve that secret by name
// (two-phase bind); otherwise only the default secret of the type is sent.
func bindWithSecret[A secretNamed](params *vgi.BindParams, schema *arrow.Schema) (*vgi.BindResponse, error) {
	var a A
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if name := a.secret(); name != "" && !params.ResolvedSecretsProvided {
		return &vgi.BindResponse{SecretScopeRequest: []vgi.SecretLookup{{SecretType: SecretType, SecretName: name}}}, nil
	}
	return vgi.BindSchema(schema)
}

// conn is a resolved connection. It is built per call from the secret and is
// never stored in scan state.
type conn struct {
	BaseURL  string
	Headers  http.Header
	AMQPURL  string
	Username string
	Password string
	Bearer   string
	TLS      *tls.Config
	client   *http.Client
}

// secretFields picks the secret (the named one when name is non-empty).
func secretFields(secrets vgi.Secrets, name string) (map[string]interface{}, error) {
	if name != "" {
		fields, ok := secrets.LookupNamed(SecretType, name)
		if !ok {
			return nil, fmt.Errorf("no rabbitmq secret named %q: create it with CREATE SECRET %s (TYPE rabbitmq, MANAGEMENT_URL 'http://localhost:15672', USERNAME '...', PASSWORD '...')", name, name)
		}
		return fields, nil
	}
	fields, ok := secrets.Lookup(SecretType)
	if !ok {
		return nil, errors.New("no rabbitmq secret found: create one with CREATE SECRET (TYPE rabbitmq, MANAGEMENT_URL 'http://localhost:15672', USERNAME '...', PASSWORD '...'); use BEARER_TOKEN instead of USERNAME/PASSWORD for OAuth 2, and AMQP_URL for stream_messages")
	}
	return fields, nil
}

// resolveConn turns the secret into a connection. needHTTP=false skips the
// management_url check (stream_messages only needs amqp_url).
func resolveConn(secrets vgi.Secrets, name string, needHTTP bool) (*conn, error) {
	fields, err := secretFields(secrets, name)
	if err != nil {
		return nil, err
	}
	get := func(k string) string { return strings.TrimSpace(vgi.RenderSecretValue(fields[k])) }
	c := &conn{
		BaseURL:  strings.TrimRight(get("management_url"), "/"),
		AMQPURL:  get("amqp_url"),
		Username: get("username"),
		Password: vgi.RenderSecretValue(fields["password"]),
		Bearer:   get("bearer_token"),
	}
	if c.TLS, err = tlsConfig(get); err != nil {
		return nil, err
	}
	if needHTTP {
		if c.BaseURL == "" {
			return nil, errors.New("the rabbitmq secret has no MANAGEMENT_URL (e.g. 'http://localhost:15672')")
		}
		u, err := url.Parse(c.BaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("the rabbitmq secret's MANAGEMENT_URL must be an http(s) URL, got %q", c.BaseURL)
		}
		c.BaseURL = strings.TrimSuffix(c.BaseURL, "/api")
	}
	h := http.Header{}
	h.Set("Accept", "application/json")
	h.Set("User-Agent", userAgent)
	if c.Bearer != "" {
		h.Set("Authorization", "Bearer "+c.Bearer)
	} else if c.Username != "" {
		req := &http.Request{Header: http.Header{}}
		req.SetBasicAuth(c.Username, c.Password)
		h.Set("Authorization", req.Header.Get("Authorization"))
	}
	c.Headers = h
	c.client = httpClient(c.TLS, get)
	return c, nil
}

// tlsConfig builds the TLS options shared by HTTPS and AMQPS; nil when none are set.
func tlsConfig(get func(string) string) (*tls.Config, error) {
	ca, cert, key, sni, skip := get("tls_ca_cert"), get("tls_client_cert"), get("tls_client_key"), get("tls_server_name"), get("tls_skip_verify")
	if ca == "" && cert == "" && key == "" && sni == "" && skip == "" {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: sni}
	switch strings.ToLower(skip) {
	case "", "false", "0", "no":
	case "true", "1", "yes":
		cfg.InsecureSkipVerify = true //nolint:gosec // explicit opt-in from the secret, documented as testing only
	default:
		return nil, fmt.Errorf("the rabbitmq secret's TLS_SKIP_VERIFY must be 'true' or 'false', got %q", skip)
	}
	if ca != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, errors.New("the rabbitmq secret's TLS_CA_CERT holds no PEM certificate")
		}
		cfg.RootCAs = pool
	}
	if (cert == "") != (key == "") {
		return nil, errors.New("the rabbitmq secret needs both TLS_CLIENT_CERT and TLS_CLIENT_KEY for mutual TLS")
	}
	if cert != "" {
		pair, err := tls.X509KeyPair([]byte(cert), []byte(key))
		if err != nil {
			return nil, fmt.Errorf("the rabbitmq secret's client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

// clients caches one http.Client per distinct TLS option set, so connections
// are reused across pages and calls.
var clients sync.Map

var defaultClient = &http.Client{Timeout: Timeout}

func httpClient(cfg *tls.Config, get func(string) string) *http.Client {
	if cfg == nil {
		return defaultClient
	}
	key := strings.Join([]string{get("tls_ca_cert"), get("tls_client_cert"), get("tls_client_key"), get("tls_server_name"), get("tls_skip_verify")}, "\x00")
	if c, ok := clients.Load(key); ok {
		return c.(*http.Client)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = cfg
	c := &http.Client{Timeout: Timeout, Transport: tr}
	actual, _ := clients.LoadOrStore(key, c)
	return actual.(*http.Client)
}

// apiError is a failed API call. The message names the HTTP status and the
// broker's error and reason.
type apiError struct {
	Status  int
	Path    string
	Message string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("rabbitmq %s: HTTP %d", e.Path, e.Status)
	switch e.Status {
	case http.StatusUnauthorized:
		msg += " (check the secret's USERNAME/PASSWORD or BEARER_TOKEN)"
	case http.StatusForbidden:
		msg += " (the user lacks the management tag or vhost permission this endpoint needs)"
	}
	if e.Message != "" {
		msg += ": " + e.Message
	}
	return msg
}

// sleep is replaced in tests so retries don't wait.
var sleep = func(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// retryDelay honours a Retry-After header (seconds or HTTP date), else backs off exponentially.
func retryDelay(header string, attempt int) time.Duration {
	header = strings.TrimSpace(header)
	if s, err := strconv.Atoi(header); err == nil && s >= 0 {
		return min(time.Duration(s)*time.Second, 30*time.Second)
	}
	if t, err := http.ParseTime(header); err == nil {
		return min(max(time.Until(t), 0), 30*time.Second)
	}
	return time.Duration(250<<attempt) * time.Millisecond
}

func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

// encodeParams sorts keys so request URLs are deterministic (recorded
// fixtures key on them).
func encodeParams(params url.Values) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		for _, v := range params[k] {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(url.QueryEscape(k))
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(v))
		}
	}
	return b.String()
}

// seg escapes one path segment; a vhost "/" becomes %2F as the API requires.
func seg(s string) string { return url.PathEscape(s) }

// reqOpts tunes one request.
type reqOpts struct {
	method string // GET by default
	body   any    // JSON body for POST
	// healthCheck: a 503 whose body is {"status":"failed",...} is the check's
	// answer, not an outage, so it is returned instead of retried.
	healthCheck bool
}

// call sends one request to <base>/api<path>?params, retrying 429/502/503/504
// (honouring Retry-After) and network errors, and returns the status and body.
func (c *conn) call(ctx context.Context, path string, params url.Values, o reqOpts) (int, []byte, error) {
	target := c.BaseURL + "/api" + path
	if q := encodeParams(params); q != "" {
		target += "?" + q
	}
	method := o.method
	if method == "" {
		method = http.MethodGet
	}
	var payload []byte
	if o.body != nil {
		var err error
		if payload, err = json.Marshal(o.body); err != nil {
			return 0, nil, err
		}
	}
	for attempt := 1; ; attempt++ {
		var rdr io.Reader
		if payload != nil {
			rdr = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, method, target, rdr)
		if err != nil {
			return 0, nil, err
		}
		for k, vs := range c.Headers {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := c.client.Do(req)
		if err != nil {
			if attempt >= MaxAttempts || errors.Is(err, context.Canceled) {
				return 0, nil, fmt.Errorf("rabbitmq /api%s: %w", path, err)
			}
			sleep(ctx, retryDelay("", attempt))
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<30))
		_ = resp.Body.Close()
		if readErr != nil {
			return 0, nil, fmt.Errorf("rabbitmq /api%s: reading response: %w", path, readErr)
		}
		if o.healthCheck && resp.StatusCode == http.StatusServiceUnavailable && healthFailed(body) {
			return resp.StatusCode, body, nil
		}
		if retryable(resp.StatusCode) && attempt < MaxAttempts {
			slog.Debug("rabbitmq: retrying", "path", path, "status", resp.StatusCode, "attempt", attempt)
			sleep(ctx, retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		return resp.StatusCode, body, nil
	}
}

// healthFailed reports whether a body is a health check's failure answer.
func healthFailed(body []byte) bool {
	var m map[string]any
	return json.Unmarshal(body, &m) == nil && m["status"] == "failed"
}

// errorFrom builds an apiError from a non-2xx response.
func errorFrom(path string, status int, body []byte) error {
	e := &apiError{Status: status, Path: "/api" + path}
	var m map[string]any
	if json.Unmarshal(body, &m) == nil {
		parts := []string{}
		for _, k := range []string{"error", "reason"} {
			if s, ok := m[k].(string); ok && s != "" {
				parts = append(parts, s)
			}
		}
		e.Message = strings.Join(parts, ": ")
	} else {
		snippet := strings.TrimSpace(string(body))
		if len(snippet) > 300 {
			snippet = snippet[:300]
		}
		e.Message = snippet
	}
	return e
}

// getJSON GETs a path and decodes the JSON body (numbers kept exact).
func (c *conn) getJSON(ctx context.Context, path string, params url.Values) (any, error) {
	status, body, err := c.call(ctx, path, params, reqOpts{})
	if err != nil {
		return nil, err
	}
	if status/100 != 2 {
		return nil, errorFrom(path, status, body)
	}
	return decodeJSON(path, body)
}

func decodeJSON(path string, body []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("rabbitmq /api%s: decoding response: %w", path, err)
	}
	return v, nil
}

// getList GETs a path whose answer is a JSON array of objects.
func (c *conn) getList(ctx context.Context, path string, params url.Values) ([]map[string]any, error) {
	v, err := c.getJSON(ctx, path, params)
	if err != nil {
		return nil, err
	}
	return asObjects(path, v)
}

func asObjects(path string, v any) ([]map[string]any, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("rabbitmq /api%s: expected a JSON array, got %T", path, v)
	}
	out := make([]map[string]any, 0, len(arr))
	for _, x := range arr {
		if m, ok := x.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out, nil
}

// page is one page of a paginated listing.
type page struct {
	Items     []map[string]any
	PageCount int64
}

// getPage GETs one page (?page=N&page_size=M plus name filters). The broker
// answers 400 "page out of range" for page 1 of an empty listing.
func (c *conn) getPage(ctx context.Context, path string, params url.Values, pageNum, pageSize int64) (*page, error) {
	p := url.Values{}
	for k, v := range params {
		p[k] = v
	}
	p.Set("page", strconv.FormatInt(pageNum, 10))
	p.Set("page_size", strconv.FormatInt(pageSize, 10))
	status, body, err := c.call(ctx, path, p, reqOpts{})
	if err != nil {
		return nil, err
	}
	if status == http.StatusBadRequest && bytes.Contains(body, []byte("out of range")) {
		return &page{}, nil
	}
	if status/100 != 2 {
		return nil, errorFrom(path, status, body)
	}
	v, err := decodeJSON(path, body)
	if err != nil {
		return nil, err
	}
	if arr, ok := v.([]any); ok {
		// Some scoped endpoints (e.g. /api/vhosts/{vhost}/connections) ignore
		// pagination and answer with the whole list: one page.
		items, err := asObjects(path, arr)
		return &page{Items: items, PageCount: pageNum}, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("rabbitmq /api%s: expected a paginated object, got %T", path, v)
	}
	items, err := asObjects(path, m["items"])
	if err != nil {
		return nil, err
	}
	pc, _ := toInt(m["page_count"])
	return &page{Items: items, PageCount: pc}, nil
}
