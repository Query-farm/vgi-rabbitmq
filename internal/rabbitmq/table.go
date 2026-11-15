// A small framework for the Management API table functions: declare the
// arguments, the columns and a fetch that returns rows of Go values for one
// page; the shared code handles state, secrets, pagination and Arrow building.
//
// The page cursor lives in the exported Page field of ListState, so over the
// HTTP transport each page can be served by a different request or process.

package rabbitmq

import (
	"context"
	"encoding/json"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// ListState carries the bound arguments (JSON), the next page to fetch
// (1-based) and whether the scan is finished.
type ListState struct {
	ArgsJSON string
	Page     int64
	Done     bool
}

// listFunc is a table function with argument struct A. fetch returns the rows
// of one page and whether another page follows; unpaginated endpoints return
// more=false.
type listFunc[A secretNamed] struct {
	name     string
	meta     func() vgi.FunctionMetadata
	columns  []arrow.Field
	validate func(A) error
	fetch    func(ctx context.Context, c *conn, a A, page int64) (rows [][]any, more bool, err error)
}

func (f *listFunc[A]) Name() string                   { return f.name }
func (f *listFunc[A]) Metadata() vgi.FunctionMetadata { return f.meta() }
func (f *listFunc[A]) ArgumentSpecs() []vgi.ArgSpec   { var a A; return vgi.DeriveArgSpecs(a) }
func (f *listFunc[A]) schema() *arrow.Schema          { return arrow.NewSchema(f.columns, nil) }
func (f *listFunc[A]) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if f.validate != nil {
		var a A
		if err := vgi.BindArgs(params.Args, &a); err != nil {
			return nil, err
		}
		if err := f.validate(a); err != nil {
			return nil, err
		}
	}
	return bindWithSecret[A](params, f.schema())
}

func (f *listFunc[A]) NewState(params *vgi.ProcessParams) (*ListState, error) {
	var a A
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if f.validate != nil {
		if err := f.validate(a); err != nil {
			return nil, err
		}
	}
	if _, err := resolveConn(params.Secrets, a.secret(), true); err != nil {
		return nil, err
	}
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return &ListState{ArgsJSON: string(b), Page: 1}, nil
}

func (f *listFunc[A]) Process(ctx context.Context, params *vgi.ProcessParams, st *ListState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	var a A
	if err := json.Unmarshal([]byte(st.ArgsJSON), &a); err != nil {
		return err
	}
	c, err := resolveConn(params.Secrets, a.secret(), true)
	if err != nil {
		return err
	}
	rows, more, err := f.fetch(ctx, c, a, st.Page)
	if err != nil {
		return err
	}
	st.Page++
	st.Done = !more
	return emitRows(out, f.columns, rows)
}

// emitRows builds and emits one batch (an empty batch is fine).
func emitRows(out *vgirpc.OutputCollector, fields []arrow.Field, rows [][]any) error {
	cols, err := buildColumns(fields, rows)
	if err != nil {
		return err
	}
	for _, col := range cols {
		defer col.Release()
	}
	return out.EmitArrays(cols, int64(len(rows)))
}

func newList[A secretNamed](f *listFunc[A]) vgi.TableFunction {
	return vgi.AsTableFunction[ListState](f)
}

// mapRows applies a row mapper to every item.
func mapRows(items []map[string]any, row func(map[string]any) []any) [][]any {
	rows := make([][]any, 0, len(items))
	for _, it := range items {
		rows = append(rows, row(it))
	}
	return rows
}

// meta builds a function's metadata: discovery tags, result-column schema,
// examples and the secret requirement.
func meta(d docs, desc string, cols []arrow.Field, examples []vgi.CatalogExample) func() vgi.FunctionMetadata {
	return func() vgi.FunctionMetadata {
		tags := objectTags(d)
		rc := make([]resultColumn, len(cols))
		for i, f := range cols {
			c, _ := f.Metadata.GetValue("comment")
			rc[i] = resultColumn{Name: f.Name, Type: duckType(f.Type), Description: c}
		}
		tags["vgi.result_columns_schema"] = mustJSON(rc)
		return vgi.FunctionMetadata{
			Description:     desc,
			Stability:       vgi.StabilityVolatile,
			Tags:            withExamples(tags, examples),
			RequiredSecrets: secretRequirements,
			Examples:        examples,
		}
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ---------------------------------------------------------------- shared argument structs

// noArgs: only secret_name.
type noArgs struct {
	SecretName *string `vgi:"name=secret_name,doc=Name of the rabbitmq secret to use when several exist."`
}

func (a noArgs) secret() string { return deref(a.SecretName) }

// vhostArgs: an optional vhost scope (pushed into the request path).
type vhostArgs struct {
	Vhost      *string `vgi:"name=vhost,doc=Only this virtual host (pushed into the API path, e.g. /api/policies/{vhost}); default all vhosts."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the rabbitmq secret to use when several exist."`
}

func (a vhostArgs) secret() string { return deref(a.SecretName) }

// vhostPath is base or base/{vhost}.
func vhostPath(base string, vhost *string) string {
	if vhost == nil {
		return base
	}
	return base + "/" + seg(*vhost)
}
