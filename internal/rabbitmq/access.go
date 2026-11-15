// Access-control functions: users, permissions and topic_permissions.
//
// users never returns password hashes: only has_password and the hashing
// algorithm are exposed.

package rabbitmq

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// ---------------------------------------------------------------- users

var usersColumns = []arrow.Field{
	column("name", str, "User name.", false),
	column("tags", strList, "Management tags: administrator, monitoring, policymaker, management, impersonator.", false),
	column("has_password", boolean, "Whether the user has a password set (the hash itself is never returned).", true),
	column("hashing_algorithm", str, "Password hashing algorithm, e.g. rabbit_password_hashing_sha256.", true),
	column("limits", strMap, "Per-user limits, e.g. limits['max-connections'] (values as text).", false),
}

var usersExamples = []vgi.CatalogExample{
	{SQL: "SELECT name, tags FROM rabbitmq.main.users() WHERE list_contains(tags, 'administrator') ORDER BY name",
		Description: "Administrators"},
	{SQL: "SELECT u.name FROM rabbitmq.main.users() u ANTI JOIN rabbitmq.main.permissions() p ON p.user = u.name ORDER BY u.name",
		Description: "Users with no permission on any vhost"},
}

func newUsers() vgi.TableFunction {
	return newList(&listFunc[noArgs]{
		name: "users", columns: usersColumns,
		meta: meta(docs{Title: "Users", Category: "access",
			Keywords: "users, administrators, tags, limits, access audit, /api/users",
			DocLLM: "One row per internal user from GET /api/users: name, tags (`VARCHAR[]`), has_password, hashing_algorithm, limits `MAP`. " +
				"Password hashes are never returned. Named arg: secret_name. Needs an administrator user.",
			DocMD: "## users()\n\nInternal users with tags and limits; password hashes are never returned."},
			"Internal users with tags and limits (no password hashes)", usersColumns, usersExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, "/users", nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				hp := vBool(m["has_password"])
				if hp == nil {
					if h, ok := m["password_hash"].(string); ok {
						hp = h != ""
					}
				}
				return []any{vStr(m["name"]), vListE(m["tags"]), hp, vStrNE(m["hashing_algorithm"]), vMap(m["limits"])}
			}), false, nil
		},
	})
}

// ---------------------------------------------------------------- permissions

var permissionsColumns = []arrow.Field{
	column("user", str, "User name.", false),
	column("vhost", str, "Virtual host.", false),
	column("configure", str, "Regex of resource names the user may declare/delete.", false),
	column("write", str, "Regex of resource names the user may publish to / bind from.", false),
	column("read", str, "Regex of resource names the user may consume from / bind to.", false),
}

var permissionsExamples = []vgi.CatalogExample{
	{SQL: "SELECT user, vhost FROM rabbitmq.main.permissions() WHERE configure = '.*' AND write = '.*' AND read = '.*' ORDER BY user, vhost",
		Description: "Users with full permissions on a vhost"},
	{SQL: "SELECT user, configure, write, read FROM rabbitmq.main.permissions(vhost := 'shop') ORDER BY user",
		Description: "Who can do what in one vhost"},
}

func newPermissions() vgi.TableFunction {
	return newList(&listFunc[vhostArgs]{
		name: "permissions", columns: permissionsColumns,
		meta: meta(docs{Title: "Vhost Permissions", Category: "access",
			Keywords: "permissions, access control, configure, write, read, audit, /api/permissions",
			DocLLM: "One row per (user, vhost) permission from GET /api/permissions or /api/vhosts/{vhost}/permissions: user, vhost and " +
				"the configure/write/read regexes ('' or '^$' = nothing, '.*' = everything). Named args: vhost (pushed into the path), secret_name.",
			DocMD: "## permissions(vhost := NULL)\n\nConfigure/write/read permission regexes per user and vhost."},
			"Configure, write and read permission regexes per user and vhost", permissionsColumns, permissionsExamples),
		fetch: func(ctx context.Context, c *conn, a vhostArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, clientScoped("permissions", a.Vhost), nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				return []any{vStr(m["user"]), vStr(m["vhost"]), vStr(m["configure"]), vStr(m["write"]), vStr(m["read"])}
			}), false, nil
		},
	})
}

// ---------------------------------------------------------------- topic_permissions

var topicPermissionsColumns = []arrow.Field{
	column("user", str, "User name.", false),
	column("vhost", str, "Virtual host.", false),
	column("exchange", str, "Topic exchange the permission applies to.", false),
	column("write", str, "Regex of routing keys the user may publish with.", false),
	column("read", str, "Regex of routing keys the user may bind with.", false),
}

var topicPermissionsExamples = []vgi.CatalogExample{
	{SQL: "SELECT user, vhost, exchange, write, read FROM rabbitmq.main.topic_permissions() ORDER BY user, vhost, exchange",
		Description: "Routing-key restrictions on topic exchanges"},
	{SQL: "SELECT exchange, count(*) AS users FROM rabbitmq.main.topic_permissions(vhost := 'shop') GROUP BY exchange ORDER BY exchange",
		Description: "How many users are restricted on each topic exchange"},
}

func newTopicPermissions() vgi.TableFunction {
	return newList(&listFunc[vhostArgs]{
		name: "topic_permissions", columns: topicPermissionsColumns,
		meta: meta(docs{Title: "Topic Permissions", Category: "access",
			Keywords: "topic permissions, routing key authorisation, access control, /api/topic-permissions",
			DocLLM: "One row per topic permission from GET /api/topic-permissions or /api/vhosts/{vhost}/topic-permissions: user, vhost, " +
				"exchange and the write/read routing-key regexes. Named args: vhost (pushed into the path), secret_name.",
			DocMD: "## topic_permissions(vhost := NULL)\n\nRouting-key permissions on topic exchanges."},
			"Routing-key permissions on topic exchanges per user and vhost", topicPermissionsColumns, topicPermissionsExamples),
		fetch: func(ctx context.Context, c *conn, a vhostArgs, _ int64) ([][]any, bool, error) {
			items, err := c.getList(ctx, clientScoped("topic-permissions", a.Vhost), nil)
			if err != nil {
				return nil, false, err
			}
			return mapRows(items, func(m map[string]any) []any {
				return []any{vStr(m["user"]), vStr(m["vhost"]), vStr(m["exchange"]), vStr(m["write"]), vStr(m["read"])}
			}), false, nil
		},
	})
}
