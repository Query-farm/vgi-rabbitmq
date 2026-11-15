// JSON value helpers and the generic Arrow builder used by every table function.
//
// Management API objects are decoded with json.Number (so 64-bit counters
// stay exact) into map[string]any. The helpers below read typed values out of
// them, returning nil for absent or null fields so they become SQL NULLs.

package rabbitmq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Arrow types used by the output schemas.
var (
	str      = arrow.BinaryTypes.String
	i64      = arrow.PrimitiveTypes.Int64
	f64      = arrow.PrimitiveTypes.Float64
	boolean  = arrow.FixedWidthTypes.Boolean
	blob     = arrow.BinaryTypes.Binary
	tstz     = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	strList  = arrow.ListOf(arrow.BinaryTypes.String)
	strMap   = arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	duckType = func(t arrow.DataType) string {
		switch t.ID() {
		case arrow.STRING:
			return "VARCHAR"
		case arrow.INT64:
			return "BIGINT"
		case arrow.FLOAT64:
			return "DOUBLE"
		case arrow.BOOL:
			return "BOOLEAN"
		case arrow.BINARY:
			return "BLOB"
		case arrow.TIMESTAMP:
			return "TIMESTAMP WITH TIME ZONE"
		case arrow.LIST:
			return "VARCHAR[]"
		case arrow.MAP:
			return "MAP(VARCHAR, VARCHAR)"
		}
		return t.String()
	}
)

// at walks nested objects: at(m, "message_stats", "publish_details", "rate").
func at(m map[string]any, path ...string) any {
	var cur any = m
	for _, p := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = obj[p]
	}
	return cur
}

// vStr is a string field, nil when absent.
func vStr(v any) any {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	}
	return nil
}

// vStrNE is a string field, nil when absent or empty.
func vStrNE(v any) any {
	if s, ok := vStr(v).(string); ok && s != "" {
		return s
	}
	return nil
}

func toInt(v any) (int64, bool) {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, true
		}
		if f, err := x.Float64(); err == nil {
			return int64(f), true
		}
	case float64:
		return int64(x), true
	case int64:
		return x, true
	case string:
		if i, err := strconv.ParseInt(x, 10, 64); err == nil {
			return i, true
		}
	}
	return 0, false
}

// vInt is an integer field, nil when absent.
func vInt(v any) any {
	if i, ok := toInt(v); ok {
		return i
	}
	return nil
}

// vFloat is a numeric field as DOUBLE, nil when absent.
func vFloat(v any) any {
	switch x := v.(type) {
	case json.Number:
		if f, err := x.Float64(); err == nil {
			return f
		}
	case float64:
		return x
	}
	return nil
}

// vBool is a boolean field, nil when absent.
func vBool(v any) any {
	if b, ok := v.(bool); ok {
		return b
	}
	return nil
}

// vBoolF is a boolean field defaulting to false.
func vBoolF(v any) any {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

// vList is a list of strings (non-string items rendered as text), nil when absent.
func vList(v any) any {
	arr, ok := v.([]any)
	if !ok {
		if s, ok := v.(string); ok && s != "" { // tags are sometimes a comma string
			parts := []string{}
			for _, p := range strings.Split(s, ",") {
				if p = strings.TrimSpace(p); p != "" {
					parts = append(parts, p)
				}
			}
			return parts
		}
		if v == nil {
			return nil
		}
		return []string{}
	}
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		out = append(out, renderValue(x))
	}
	return out
}

// vListE is vList with an empty list instead of NULL.
func vListE(v any) any {
	if l := vList(v); l != nil {
		return l
	}
	return []string{}
}

// renderValue renders a JSON value as text: strings as-is, numbers and
// booleans in their JSON form, objects and arrays as compact JSON.
func renderValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// vMap is an object as MAP(VARCHAR, VARCHAR) with values rendered as text;
// empty map when absent (arguments, definitions and headers are always maps).
func vMap(v any) any {
	out := map[string]string{}
	if m, ok := v.(map[string]any); ok {
		for k, x := range m {
			if x == nil {
				continue
			}
			out[k] = renderValue(x)
		}
	}
	return out
}

// vJSON is a value as compact JSON text, nil when absent.
func vJSON(v any) any {
	if v == nil {
		return nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // keep '&' in URIs readable
	if err := enc.Encode(v); err != nil {
		return nil
	}
	return strings.TrimRight(buf.String(), "\n")
}

// errInvalid is an argument error shown to the user as-is.
func errInvalid(msg string) error { return fmt.Errorf("%s", msg) }

// timeLayouts are the formats the management API uses for times.
var timeLayouts = []string{time.RFC3339Nano, "2006-01-02T15:04:05.000-07:00", "2006-01-02 15:04:05", "2006-01-02T15:04:05"}

// vTime parses a time string (RFC 3339 or "YYYY-MM-DD hh:mm:ss" UTC), nil when absent.
func vTime(v any) any {
	s, ok := v.(string)
	if !ok || s == "" {
		return nil
	}
	for _, l := range timeLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC()
		}
	}
	return nil
}

// vEpochMs is a Unix-millisecond number as a time, nil when absent or 0.
func vEpochMs(v any) any {
	if i, ok := toInt(v); ok && i > 0 {
		return time.UnixMilli(i).UTC()
	}
	return nil
}

// vEpochS is a Unix-second number as a time, nil when absent or 0.
func vEpochS(v any) any {
	if i, ok := toInt(v); ok && i > 0 {
		return time.Unix(i, 0).UTC()
	}
	return nil
}

// rate reads <obj>.<key>_details.rate.
func rate(m map[string]any, obj, key string) any {
	if obj == "" {
		return vFloat(at(m, key+"_details", "rate"))
	}
	return vFloat(at(m, obj, key+"_details", "rate"))
}

// buildColumns builds Arrow columns from rows of Go values. Supported values
// per column type: string; int64; float64; bool; []byte; time.Time;
// []string; map[string]string; nil for NULL.
func buildColumns(fields []arrow.Field, rows [][]any) ([]arrow.Array, error) {
	mem := memory.NewGoAllocator()
	out := make([]arrow.Array, 0, len(fields))
	release := func() {
		for _, a := range out {
			a.Release()
		}
	}
	for ci, fd := range fields {
		b := array.NewBuilder(mem, fd.Type)
		for ri, r := range rows {
			v := r[ci]
			if v == nil && !fd.Nullable {
				// A field the API omitted: '' / false for required text and flags.
				switch fd.Type.ID() {
				case arrow.STRING:
					v = ""
				case arrow.BOOL:
					v = false
				}
			}
			if v == nil {
				if !fd.Nullable {
					b.Release()
					release()
					return nil, fmt.Errorf("internal: NULL in non-null column %s (row %d)", fd.Name, ri)
				}
				b.AppendNull()
				continue
			}
			if err := appendValue(b, v); err != nil {
				b.Release()
				release()
				return nil, fmt.Errorf("column %s: %w", fd.Name, err)
			}
		}
		out = append(out, b.NewArray())
		b.Release()
	}
	return out, nil
}

func appendValue(b array.Builder, v any) error {
	switch bb := b.(type) {
	case *array.StringBuilder:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("want string, got %T", v)
		}
		bb.Append(s)
	case *array.BinaryBuilder:
		x, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("want bytes, got %T", v)
		}
		bb.Append(x)
	case *array.Int64Builder:
		i, ok := v.(int64)
		if !ok {
			return fmt.Errorf("want int64, got %T", v)
		}
		bb.Append(i)
	case *array.Float64Builder:
		f, ok := v.(float64)
		if !ok {
			return fmt.Errorf("want float64, got %T", v)
		}
		bb.Append(f)
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("want bool, got %T", v)
		}
		bb.Append(x)
	case *array.TimestampBuilder:
		t, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("want time, got %T", v)
		}
		bb.Append(arrow.Timestamp(t.UnixMicro()))
	case *array.ListBuilder:
		l, ok := v.([]string)
		if !ok {
			return fmt.Errorf("want []string, got %T", v)
		}
		bb.Append(true)
		vb := bb.ValueBuilder().(*array.StringBuilder)
		for _, s := range l {
			vb.Append(s)
		}
	case *array.MapBuilder:
		m, ok := v.(map[string]string)
		if !ok {
			return fmt.Errorf("want map, got %T", v)
		}
		bb.Append(true)
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		kb := bb.KeyBuilder().(*array.StringBuilder)
		ib := bb.ItemBuilder().(*array.StringBuilder)
		for _, k := range keys {
			kb.Append(k)
			ib.Append(m[k])
		}
	default:
		return fmt.Errorf("unsupported builder %T", b)
	}
	return nil
}
