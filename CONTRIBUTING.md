# Contributing to Axon EventStoreDB Spring Boot Starter

Thank you for your interest in contributing! This guide will help you get started.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## How to Contribute

### Reporting Bugs

1. Check [existing issues](https://github.com/bakalr/axon-eventstoredb/issues) to avoid duplicates
2. Open a new issue with:
   - Clear title and description
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (Java version, Spring Boot version, EventStoreDB version)

### Suggesting Features

Open an issue with the `enhancement` label describing:

- The use case and motivation
- Proposed API or configuration
- Any alternatives you've considered

### Submitting Code

1. **Fork** the repository
2. **Create a branch** from `main`:

   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes** following the coding standards below
4. **Add tests** for any new functionality
5. **Run the full build**:

   ```bash
   ./gradlew build
   ```

6. **Commit** using [Conventional Commits](https://www.conventionalcommits.org/):

   ```text
   feat: add persistent subscription support
   fix: handle reconnection on network failure
   docs: update configuration reference
   test: add integration test for snapshot reading
   ```

7. **Push** and open a **Pull Request**

## Development Setup

### Prerequisites

- Java 21+
- Docker (for integration tests)
- An IDE with Gradle support (IntelliJ IDEA or VS Code recommended)

### Building

```bash
# Full build with tests
./gradlew build

# Compile only
./gradlew compileJava

# Run unit tests
./gradlew test

# Run integration tests (requires Docker)
./gradlew integrationTest
```

### Project Structure

```text
src/
├── main/java/com/bakalr/axon/eventstoredb/
│   ├── EventStoreDBEventStorageEngine.java   # Core event store implementation
│   ├── EventStoreDBTokenStore.java           # Tracking token persistence
│   ├── EventStoreDBTrackingToken.java        # Position-based tracking token
│   ├── autoconfig/
│   │   ├── AxonEventStoreDBAutoConfiguration.java
│   │   └── EventStoreDBProperties.java
│   └── util/
│       ├── EventStoreDBSerializer.java       # Axon ↔ ESDB serialization
│       └── EventStoreDBStreamNaming.java     # Stream naming conventions
├── main/resources/META-INF/spring/
│   └── org.springframework.boot.autoconfigure.AutoConfiguration.imports
└── test/java/com/bakalr/axon/eventstoredb/
    ├── EventStoreDBTrackingTokenTest.java
    ├── autoconfig/EventStoreDBPropertiesTest.java
    └── util/
        ├── EventStoreDBSerializerTest.java
        └── EventStoreDBStreamNamingTest.java
```

## Coding Standards

### Java Style

- **Java 21** — Use modern features (records, pattern matching, text blocks) where appropriate
- **No Lombok** — Keep the dependency footprint minimal
- **Javadoc** — All public classes and methods must have Javadoc
- **Null safety** — Use `@Nonnull` / `@Nullable` annotations from `jakarta.annotation`
- **Immutability** — Prefer immutable objects, use `final` fields

### Testing

- **Unit tests** — Required for all new functionality
- **Integration tests** — Use Testcontainers with EventStoreDB for end-to-end tests
- **Naming** — `should<Expected>When<Condition>` or `should<Action><Result>`
- **Assertions** — Prefer AssertJ for readable assertions

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | Usage |
| -------- | ------- |
| `feat:` | New feature |
| `fix:` | Bug fix |
| `docs:` | Documentation changes |
| `test:` | Adding or updating tests |
| `refactor:` | Code refactoring (no behavior change) |
| `chore:` | Build, CI, or tooling changes |
| `perf:` | Performance improvement |

## Areas for Contribution

Here are some areas where contributions are especially welcome:

- **Integration tests** with Testcontainers + EventStoreDB
- **Subscription-based tracking** using EventStoreDB persistent subscriptions
- **Distributed token claims** for multi-node deployments
- **Event upcasting** support
- **Metrics** via Micrometer
- **Documentation** improvements and examples
- **CI/CD** pipeline setup (GitHub Actions)

## Questions?

Open a [Discussion](https://github.com/bakalr/axon-eventstoredb/discussions) or reach out via issues.

Thank you for contributing!
