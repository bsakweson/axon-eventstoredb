# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-02-10

### Added

- `EventStoreDBPersistentSubscriptionMessageSource` — Push-based `StreamableMessageSource` backed by EventStoreDB persistent subscriptions with competing consumer support
- `DistributedTokenClaimManager` — Distributed token claim management using EventStoreDB optimistic concurrency for multi-node deployments
  - Claim acquisition with optimistic concurrency to enforce single-owner semantics
  - Claim expiry with configurable timeout for automatic failover
  - Claim extension to prevent expiry during active processing
  - Explicit claim release for graceful shutdown
- `EventStoreDBTokenStore` integration with `DistributedTokenClaimManager` for optional distributed claims
- Auto-configuration for `DistributedTokenClaimManager` via `axon.eventstoredb.claims.enabled`
- Auto-configuration for `EventStoreDBPersistentSubscriptionMessageSource` via `axon.eventstoredb.subscription.enabled`
- Auto-configuration for `EventStoreDBPersistentSubscriptionsClient` when subscriptions are enabled
- `EventStoreDBProperties.Claims` configuration class (`claims.enabled`, `claims.timeout-seconds`)
- `EventStoreDBProperties.Subscription` configuration class (`subscription.enabled`, `subscription.group-name`, `subscription.buffer-size`, `subscription.create-if-not-exists`)
- Release pipeline auto-updates README dependency versions and CHANGELOG on release
- Phase 3 integration tests (28 tests) covering persistent subscriptions, distributed claims, claim expiry, and multi-segment scenarios

### Fixed

- Metric name in `shouldRecordClaimMetrics` test (`token.operations` → `tokens.operations`, tag `operation` → `type`)

## [0.2.1] - 2026-02-10

### Fixed

- `TrackedEventData` upcasting bug

### Added

- Phase 2 integration tests

## [0.2.0] - 2026-02-10

### Added

- `EventStoreDBMetrics` — Micrometer instrumentation for append/read/token/error/retry operations
- `RetryPolicy` and `EventStoreDBRetryExecutor` — Configurable retry with exponential backoff and jitter
- `EventStoreDBDomainEventData` and `EventStoreDBTrackedDomainEventData` — Axon `EventUpcaster` support
- `EventStoreDBStreamNaming` — Configurable stream naming with custom prefix support
- Auto-configuration for retry (`axon.eventstoredb.retry.*`) and metrics (`axon.eventstoredb.metrics.*`)

## [0.1.3] - 2026-02-09

### Fixed

- Use GitHub App token in release workflow to bypass branch protection
- Change group ID to `io.github.bsakweson` for Central Portal
- Switch to NMCP settings plugin v1.4.4 for Central Portal publishing
- Migrate publishing from legacy OSSRH to Central Portal
- Correct 3 failing integration tests
- Resolve all checkstyle violations in test sources

### Added

- CODEOWNERS and branch protection

## [0.1.0] - 2026-02-09

### Added

- `EventStoreDBEventStorageEngine` — Full `EventStorageEngine` implementation backed by EventStoreDB
  - Aggregate event streams with optimistic concurrency via `ExpectedRevision`
  - Global `$all` stream reading for tracking processors
  - Snapshot storage in dedicated `__snapshot-{Type}-{Id}` streams
  - Token creation (`createTailToken`, `createHeadToken`, `createTokenAt`)
  - Sequence number tracking per aggregate
- `EventStoreDBTokenStore` — `TokenStore` implementation persisting tokens in EventStoreDB streams
  - Append-only token history for debugging
  - Segment initialization and enumeration
  - Lightweight in-process claim management
- `EventStoreDBTrackingToken` — `TrackingToken` mapping EventStoreDB's `(commitPosition, preparePosition)` to Axon's tracking model
- `EventStoreDBSerializer` — Serialization bridge between Axon's `Serializer` and EventStoreDB's `EventData`/`RecordedEvent`
- `EventStoreDBStreamNaming` — Configurable stream naming conventions (`{Type}-{Id}` for aggregates)
- `AxonEventStoreDBAutoConfiguration` — Spring Boot auto-configuration with `@ConditionalOnProperty`
- `EventStoreDBProperties` — Configuration properties under `axon.eventstoredb.*`
- 31 unit tests covering all components
- Apache License 2.0
- README with quick start, configuration reference, and architecture documentation
- Contributing guidelines

[Unreleased]: https://github.com/bsakweson/axon-eventstoredb/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/bsakweson/axon-eventstoredb/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/bsakweson/axon-eventstoredb/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/bsakweson/axon-eventstoredb/compare/v0.1.0...v0.1.3
[0.1.0]: https://github.com/bsakweson/axon-eventstoredb/releases/tag/v0.1.0
