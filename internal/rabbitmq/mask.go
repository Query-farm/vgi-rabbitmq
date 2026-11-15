// Credential masking for runtime parameters (shovels, federation upstreams)
// and anything else that can carry a URI with a password in it.

package rabbitmq

import (
	"regexp"
	"strings"
)

// Mask replaces every masked secret.
const Mask = "****"

var (
	// scheme://user:password@host -> scheme://user:****@host
	uriPassword = regexp.MustCompile(`(?i)([a-z][a-z0-9+.\-]*://)([^:/@\s]*):([^@/\s]*)@`)
	// ?password=x / &token=x query parameters.
	queryPassword = regexp.MustCompile(`(?i)([?&](?:password|passwd|pass|secret|token|access_token|api_key|apikey)=)([^&\s#]+)`)
)

// sensitiveKey reports whether an object key names a credential.
func sensitiveKey(k string) bool {
	k = strings.ToLower(k)
	for _, s := range []string{"password", "passwd", "secret", "token", "private_key", "api_key", "apikey", "credential"} {
		if strings.Contains(k, s) {
			return true
		}
	}
	return false
}

// maskString hides passwords in URIs and query strings.
func maskString(s string) string {
	s = uriPassword.ReplaceAllString(s, "${1}${2}:"+Mask+"@")
	return queryPassword.ReplaceAllString(s, "${1}"+Mask)
}

// maskValue returns a copy of a decoded JSON value with every URI password,
// password query parameter and credential-named key masked.
func maskValue(v any) any {
	switch x := v.(type) {
	case string:
		return maskString(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = maskValue(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			if sensitiveKey(k) && e != nil {
				out[k] = Mask
				continue
			}
			out[k] = maskValue(e)
		}
		return out
	}
	return v
}
