# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/bakalr/axon-eventstoredb/commits/main
