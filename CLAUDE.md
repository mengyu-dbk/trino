# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Trino is a fast distributed SQL query engine for big data analytics. It uses a plugin architecture where connectors allow Trino to query data from various sources (Hive, Iceberg, PostgreSQL, MySQL, Kafka, etc.).

## Build System

**Maven-based build using Java 24**

- Build command: `./mvnw clean install -DskipTests`
- Validate (checkstyle, formatting, etc.): `./mvnw validate`
- Sort POM files: `./mvnw sortpom:sort`
- Generate license headers: `./mvnw license:format`
- Run with Error Prone compiler: `./mvnw clean install -DskipTests -Perrorprone-compiler`

The first build downloads dependencies and caches them in `~/.m2/repository`, which takes time. Subsequent builds are faster.

## Running Tests

**Tests are disabled by default** due to their comprehensive nature and long runtime. The CI runs them on PRs.

Run tests for specific modules:
```bash
# Run tests in a single module
./mvnw test -pl plugin/trino-iceberg

# Run a specific test class
./mvnw test -pl plugin/trino-iceberg -Dtest=TestIcebergConnectorTest

# Run tests for areas you modified
./mvnw test -pl core/trino-main,plugin/trino-iceberg
```

Use the `gib` (Git Incremental Builder) profile to build only changed modules:
```bash
./mvnw clean install -Pgib
```

## Running Trino in Development

### Quick Start with TpchQueryRunner

The simplest way to run Trino for development:

1. In IntelliJ, run the `TpchQueryRunner` class (in `testing/trino-tests/src/test/java/io/trino/tests/tpch/TpchQueryRunner.java`)
2. This starts a development server with the TPCH connector pre-configured
3. Each connector has its own `*QueryRunner` class (e.g., `IcebergQueryRunner`, `HiveQueryRunner`, `PostgreSqlQueryRunner`)

### Running Full Development Server

Create a run configuration in IntelliJ:
- **Main Class**: `io.trino.server.DevelopmentServer`
- **VM Options**: `-ea -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true`
- **Working directory**: `$MODULE_DIR$`
- **Module classpath**: `trino-server-dev`

Configuration is in `testing/trino-server-dev/etc/config.properties`. To enable additional plugins, modify the `plugin.bundles` property (entries can be paths to `pom.xml` files, Maven coordinates, or plugin directories).

For catalogs, add `<catalog_name>.properties` files to `testing/trino-server-dev/etc/catalog/`.

### Using the CLI

After building, run the CLI:
```bash
client/trino-cli/target/trino-cli-*-executable.jar
```

Example queries:
```sql
SELECT * FROM system.runtime.nodes;
SELECT * FROM tpch.tiny.region;
```

## Architecture

### Core Modules (in `core/`)

- **trino-spi**: Service Provider Interface - the plugin API for extending Trino. This defines interfaces for connectors, functions, types, and other extension points.
- **trino-main**: Main query engine including planner, optimizer, scheduler, and execution engine. Contains the logical and physical plan representations.
- **trino-parser**: SQL parser built on ANTLR4 (grammar in `trino-grammar`). Converts SQL text into AST.
- **trino-server**: Server packaging and deployment infrastructure. Orchestrates the entire query lifecycle.

### Plugin Architecture

Plugins are in `plugin/` and include:
- **Connectors**: Data source integrations (e.g., `trino-iceberg`, `trino-hive`, `trino-postgresql`, `trino-mysql`)
- **Functions**: Additional function libraries (e.g., `trino-ml`, `trino-geospatial`, `trino-teradata-functions`)
- **Event Listeners**: Query logging and monitoring (e.g., `trino-http-event-listener`, `trino-kafka-event-listener`)
- **Resource Group Managers**: Query resource management
- **Exchange Managers**: Intermediate data exchange (e.g., `trino-exchange-filesystem`)

Each plugin is a separate Maven module with its own `Plugin` implementation that registers connectors, functions, types, etc.

### Library Modules (in `lib/`)

Shared infrastructure used across connectors:
- **trino-filesystem**: Abstraction layer for file systems (S3, HDFS, Azure, GCS)
- **trino-orc**, **trino-parquet**: File format readers/writers
- **trino-hive-formats**: Support for Hive file formats (RCFile, Avro, etc.)
- **trino-metastore**: Hive metastore client implementations
- **trino-plugin-toolkit**: Common utilities for building plugins

### Testing Infrastructure (in `testing/`)

- **trino-testing**: Base testing framework including `QueryRunner` and assertion utilities
- **trino-tests**: Integration test suites
- **trino-server-dev**: Development server configuration
- **trino-product-tests**: End-to-end product tests
- **trino-testing-containers**: Testcontainers-based infrastructure for integration tests

## Development Workflow

### Code Style

Follow the [Airlift code style](https://github.com/airlift/codestyle). Key points:
- Use AssertJ for assertions (not JUnit assertions)
- Avoid mocking libraries - write test doubles by hand
- Prefer Guava immutable collections
- Avoid abbreviations and slang in code
- No `var` keyword
- Avoid default clause in exhaustive enum-based switch statements
- Avoid `get` prefix in method names unless it's a getter
- Use `format()` for string formatting (statically imported)

### IntelliJ Configuration

Enable Error Prone:
1. Install "Error Prone Compiler" plugin
2. Check the `errorprone-compiler` profile in Maven tab
3. Use "Javac with error-prone" as compiler

See `.github/DEVELOPMENT.md` for detailed IntelliJ inspection settings.

### Web UI Development

Web UI is in `core/trino-web-ui/src/main/resources/webapp/src/` (React/JSX/ES6).

Build Web UI:
```bash
# Full build with dependency install
yarn --cwd core/trino-web-ui/src/main/resources/webapp/src install

# Quick rebuild (no dependency changes)
yarn --cwd core/trino-web-ui/src/main/resources/webapp/src run package

# Watch mode for development
yarn --cwd core/trino-web-ui/src/main/resources/webapp/src run watch
```

Changes are hot-reloaded when you rebuild the project in IntelliJ.

## Query Execution Flow

1. **Parsing**: SQL → AST (in `trino-parser`)
2. **Analysis**: AST → analyzed statement with type information (in `trino-main`)
3. **Planning**: Analyzed statement → logical plan → optimized logical plan (in `trino-main`)
4. **Scheduling**: Physical plan fragments are distributed to workers (in `trino-main`)
5. **Execution**: Workers execute operators and exchange data (in `trino-main`)

Connectors provide metadata (table schemas, statistics) and data (via splits and page sources).

## Common Development Scenarios

### Adding a New Connector

1. Create a new module in `plugin/trino-<name>/`
2. Implement `Plugin` interface to register the connector factory
3. Implement `ConnectorFactory` to create connector instances
4. Implement `Connector` interface with metadata, split manager, page source provider
5. Add a `*QueryRunner` class for testing
6. Add integration tests using the query runner
7. Add module to root `pom.xml`

### Running a Single Test

```bash
# Run specific test method
./mvnw test -pl plugin/trino-iceberg -Dtest=TestIcebergTable#testCreateTable

# Run all tests in a class
./mvnw test -pl core/trino-main -Dtest=TestExpressionOptimizer

# Run tests matching pattern
./mvnw test -pl plugin/trino-hive -Dtest='Test*Partition*'
```

### Debugging Query Planning

1. Run a `*QueryRunner` in debug mode
2. Set breakpoints in `io.trino.sql.planner.LogicalPlanner` or optimization rules
3. Execute a query from the CLI or in a test
4. Inspect the logical plan transformations

### Working with SPI Changes

When modifying `trino-spi`:
1. Changes affect all connectors - check for compilation errors across plugins
2. The SPI has strong backward compatibility requirements
3. Run full build to ensure all connectors compile: `./mvnw clean install -DskipTests`
