// Command vgi-rabbitmq-worker serves the rabbitmq catalog to DuckDB.
//
//	vgi-rabbitmq-worker                                  # stdio (what DuckDB launches)
//	vgi-rabbitmq-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-rabbitmq-worker --unix /tmp/rabbitmq.sock         # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'rabbitmq' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-rabbitmq-worker');
package main

import (
	"os"

	"github.com/Query-farm/vgi-go/vgi"

	"github.com/Query-farm/vgi-rabbitmq/internal/rabbitmq"
	"github.com/Query-farm/vgi-rabbitmq/internal/serve"
)

func main() {
	serve.Main("vgi-rabbitmq-worker", os.Args[1:], func() *vgi.Worker { return rabbitmq.NewWorker() })
}
