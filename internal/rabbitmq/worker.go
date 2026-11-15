// Package rabbitmq exposes a RabbitMQ broker (Management HTTP API and
// non-destructive stream reads) as a VGI catalog.
//
// To add a function: write it (see broker.go, topology.go, clients.go,
// access.go or messages.go), then register it in NewWorker below.
//
// Run it:
//
//	vgi-rabbitmq-worker                      # stdio: what DuckDB launches by default
//	vgi-rabbitmq-worker --http --port 8000   # HTTP: for Docker / hosted deploys
//	vgi-rabbitmq-worker --help
//
// Attach it from DuckDB:
//
//	ATTACH 'rabbitmq' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-rabbitmq-worker');
package rabbitmq

import (
	"github.com/Query-farm/vgi-go/vgi"
)

// allExamples lists every function's examples for the schema-level tag.
func allExamples() []exampleQuery {
	return exampleQueries(overviewExamples, nodesExamples, vhostsExamples, queuesExamples, exchangesExamples,
		bindingsExamples, connectionsExamples, channelsExamples, consumersExamples, usersExamples, permissionsExamples,
		topicPermissionsExamples, policyExamples, operatorPolicyExamples, parametersExamples, globalParametersExamples,
		shovelsExamples, definitionsExamples, healthExamples, featureFlagExamples, deprecatedExamples, streamExamples,
		peekExamples)
}

// NewWorker builds the worker: one catalog, one main schema. Extra options
// (logging, storage) are appended after the catalog options.
func NewWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	sourceURL, version := SourceURL, Version
	base := []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment(CatalogDocMD),
		vgi.WithCatalogTags(CatalogTags),
		vgi.WithCatalogInfo(vgi.CatalogInfo{SourceURL: &sourceURL, ImplementationVersion: &version}),
		vgi.WithSchemaComments(map[string]string{"main": SchemaDocMD}),
		vgi.WithSchemaTags(map[string]map[string]string{"main": schemaTags(allExamples())}),
		vgi.WithSecretTypes(APISecretType),
	}
	w := vgi.NewWorker(append(base, opts...)...)
	for _, f := range []vgi.TableFunction{newOverview(), newNodes(), newVhosts(), newQueues(), newExchanges(), newBindings(),
		newConnections(), newChannels(), newConsumers(), newUsers(), newPermissions(), newTopicPermissions(),
		newUserPolicies(), newOperatorPolicies(), newParameters(), newGlobalParameters(), newShovels(),
		newDefinitionsSummary(), newHealthChecks(), newFeatureFlags(), newDeprecatedFeatures(), newPeekMessages()} {
		w.RegisterTable(f)
	}
	w.RegisterTable(vgi.AsTableFunction[StreamState](&StreamMessagesFunc{}))
	return w
}
