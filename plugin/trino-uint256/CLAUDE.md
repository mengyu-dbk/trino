# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the `trino-uint256` plugin for Trino - a 256-bit unsigned integer type implementation. The plugin provides a complete UInt256 data type with support for arithmetic operations, type conversions, aggregations, and SQL integration.

## Common Development Commands

### Building the Plugin
```bash
# Build the entire Trino project (recommended for first build)
cd /Users/emon100/IdeaProjects/trino
./mvnw clean install -DskipTests

# Build only the uint256 plugin
cd /Users/emon100/IdeaProjects/trino/plugin/trino-uint256
./mvnw clean compile

# Full build with tests
../../mvnw clean install
```

### Running Tests
```bash
# Run all tests for the plugin
../../mvnw test

# Run specific test classes
../../mvnw test -Dtest=TestUInt256Query
../../mvnw test -Dtest=TestUInt256Integration
../../mvnw test -Dtest=TestUInt256NumericCasts
../../mvnw test -Dtest=TestUInt256AggregationFunctions

# Run integration tests that require a full Trino server
../../mvnw test -Dtest=TestUInt256Integration
```

### Development Server Setup
To test the plugin with a running Trino server:

1. The plugin is already configured in the development server at `/Users/emon100/IdeaProjects/trino/testing/trino-server-dev/etc/config.properties` (line 57)
2. Run the development server using IntelliJ with:
   - Main Class: `io.trino.server.DevelopmentServer` 
   - Working directory: `trino-server-dev`
   - VM Options: `-ea -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true --sun-misc-unsafe-memory-access=allow`

### Code Quality
```bash
# Run checkstyle (inherited from parent project)
../../mvnw checkstyle:check

# Verify compilation and basic checks
../../mvnw verify -DskipTests
```

## Architecture Overview

### Plugin Structure
- **UInt256Plugin**: Main plugin class that registers the type and functions with Trino
- **UInt256Type**: Custom type implementation extending `AbstractVariableWidthType`
- **UInt256Operators**: Contains all arithmetic operations, type casts, and utility functions
- **Aggregation Package**: Sum and average aggregation functions for UInt256 values

### Key Implementation Details

#### Type System
- **Storage**: 32-byte big-endian VARBINARY representation
- **Range**: 0 to 2^256 - 1
- **Comparison**: Lexicographic comparison of normalized 32-byte values
- **Null Handling**: Standard SQL null semantics

#### Operations Supported
- **Arithmetic**: `+`, `-`, `*`, `/`, `%` with overflow detection
- **Bitwise**: AND, OR, XOR, NOT, left shift, right shift
- **Comparisons**: `=`, `<>`, `<`, `<=`, `>`, `>=`
- **Aggregations**: SUM, AVG with custom state management

#### Type Conversions
- **From UInt256**: To all numeric types (BIGINT, INTEGER, SMALLINT, TINYINT, REAL, DOUBLE, DECIMAL)
- **To UInt256**: From all numeric types, VARBINARY, VARCHAR (decimal representation)
- **Decimal Support**: Separate handling for short decimals (precision ≤ 18) and long decimals (precision > 18)

### Test Organization
- **TestUInt256Query**: Basic type operations, block I/O, arithmetic, bitwise operations
- **TestUInt256Integration**: End-to-end SQL tests with real Trino server
- **TestUInt256NumericCasts**: Comprehensive type conversion testing  
- **TestUInt256AggregationFunctions**: Aggregation function testing

## Usage Examples

### Creating UInt256 Values
```sql
-- From hex strings
CAST(from_hex('FF') AS UINT256)
uint256(from_hex('DEADBEEF'))

-- From numeric types  
CAST(123 AS UINT256)
CAST(CAST(456 AS DECIMAL(10,0)) AS UINT256)
```

### Arithmetic Operations
```sql
SELECT v1 + v2, v1 * v2, v1 / v2 
FROM table_with_uint256_columns;
```

### Aggregations
```sql
SELECT SUM(uint256_col), AVG(uint256_col)
FROM table_with_uint256_data;
```

## Testing Best Practices

- **Unit Tests**: Focus on individual operations and edge cases
- **Integration Tests**: Use `DistributedQueryRunner` for end-to-end SQL testing
- **Error Testing**: Verify overflow, underflow, and invalid conversion errors
- **Boundary Testing**: Test with 0, max value (2^256-1), and edge cases
- **Null Propagation**: Ensure proper null handling in all operations

## Development Notes

- The plugin extends Trino's SPI (Service Provider Interface)
- All arithmetic operations include overflow/underflow checking
- Type conversions reject negative values and non-integers for safety
- The implementation prioritizes correctness over performance optimizations
- SQL integration works through standard Trino type registration mechanisms
- 在聚合函数中的State中，不应该使用不可序列化的类型例如BigInteger。