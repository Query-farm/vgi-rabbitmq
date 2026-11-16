# Changelog

## 0.1.1

First complete release (v0.1.0 shipped macOS and Windows binaries only: the Linux builds
failed a release-workflow glibc check that wrongly rejected statically linked Go binaries).

- Catalog and schema descriptions rewritten for vgi-lint 2.8 (no object lists; distinct comments).

## 0.1.0

Initial RabbitMQ worker: 21 Management API table functions (read-only), stream_messages (non-destructive AMQP stream reads), opt-in peek_messages, offline topic_matches and mask_uri. Started from vgi-starter-go v1.3.0.

Targets: vgi-go 0.42 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
