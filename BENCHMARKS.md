# SCRT vs JSON vs CSV Benchmark Results

## Test Environment
- **CPU**: Apple M2 Pro
- **OS**: macOS (arm64)
- **Go**: 1.23+
- **Benchmark Command**: `go test -bench=. -benchmem`

## Data Size Comparison (Compression Ratio)

| Records | JSON Size | CSV Size | SCRT Size | SCRT vs JSON | SCRT vs CSV |
|---------|-----------|----------|-----------|--------------|-------------|
| 100     | 12,043 B  | 7,868 B  | 706 B     | **17.1x** (94.1% smaller) | **11.1x** (91.0% smaller) |
| 1,000   | 121,394 B | 79,419 B | 6,988 B   | **17.4x** (94.2% smaller) | **11.4x** (91.2% smaller) |
| 10,000  | 1,223,895 B | 803,920 B | 70,906 B | **17.3x** (94.2% smaller) | **11.3x** (91.2% smaller) |

**Key Takeaway**: SCRT achieves ~94% size reduction vs JSON and ~91% vs CSV! 🎯

## Marshal Performance (Structs)

| Records | SCRT (ns/op) | JSON (ns/op) | CSV (ns/op) | Winner | SCRT Allocs | JSON Allocs | CSV Allocs |
|---------|--------------|--------------|-------------|--------|-------------|-------------|------------|
| 100     | 16,849       | 16,005       | 14,370      | **CSV** ~1.2x faster than SCRT | 45 | 2 | 105 |
| 1,000   | 152,996      | 155,667      | 157,307     | **SCRT** edges JSON/CSV | 60 | 2 | 1,909 |
| 10,000  | 1,533,112    | 1,559,624    | 1,477,028   | **CSV** ~1.04x faster | 484 | 3 | 19,912 |

## Unmarshal Performance (Structs)

| Records | SCRT (ns/op) | JSON (ns/op) | CSV (ns/op) | Winner | SCRT Allocs | JSON Allocs | CSV Allocs |
|---------|--------------|--------------|-------------|--------|-------------|-------------|------------|
| 100     | 24,911       | 86,872       | 16,309      | **CSV** ~1.5x faster than SCRT | 333 | 214 | 226 |
| 1,000   | 224,381      | 858,201      | 155,176     | **CSV** ~1.4x faster; SCRT ~3.8x faster than JSON | 3,784 | 2,017 | 2,029 |
| 10,000  | 2,452,692    | 8,980,693    | 1,772,269   | **CSV** ~1.4x faster; SCRT ~3.7x faster than JSON | 39,876 | 20,025 | 20,036 |

**SCRT cuts unmarshalling time vs JSON by ~3.7x while CSV remains the speed leader.**

## Round-Trip Performance (1,000 records)

| Format | Time (ns/op) | Memory (B/op) | Allocations | Winner |
|--------|--------------|---------------|-------------|--------|
| SCRT   | 378,440      | 248,434       | 3,845       | **~2.7x faster than JSON; lowest B/op** |
| JSON   | 1,012,943    | 317,537       | 2,019       | - |
| CSV    | 318,171      | 551,355       | 3,938       | **Fastest** |

**CSV is fastest; SCRT is ~2.7x faster than JSON with lower bytes/op.**

## Map Performance (1,000 records)

### Marshal
- **SCRT**: 345,348 ns/op, 201,590 B/op, 10,060 allocs
- **JSON**: 629,269 ns/op, 491,410 B/op, 11,002 allocs
- **Winner**: SCRT ~1.8x faster; ~2.4x less memory

### Unmarshal
- **SCRT**: 481,336 ns/op, 600,185 B/op, 15,784 allocs
- **JSON**: 1,288,422 ns/op, 601,727 B/op, 21,006 allocs
- **Winner**: SCRT ~2.7x faster with similar memory

## Key Takeaways

### 🏆 SCRT Wins
1. **Data Size**: 94% smaller than JSON, 91% smaller than CSV (17x and 11x compression)
2. **Unmarshal Speed**: ~3.7x faster than JSON for structs
3. **Map Marshal**: ~1.9x faster than JSON with ~2.4x less memory
4. **Map Unmarshal**: ~2.6x faster than JSON
5. **Round-trip vs JSON**: ~2.7x faster with lowest bytes/op

### 🏆 CSV Wins
1. **Marshal Speed**: Fastest for small-medium datasets
2. **Unmarshal Speed**: Fastest overall (≈1.4x faster than SCRT at 1K records)
3. **Round-trip**: Fastest end-to-end

### ⚖️ Trade-offs

#### SCRT
- ✅ **Best compression**: 94% smaller than JSON, 91% smaller than CSV
- ✅ **Fast unmarshaling**: ~3.8x faster than JSON
- ✅ **Type-safe schema**: Built-in validation
- ⚠️ **Marshal speed**: Slower than CSV/JSON for small datasets
- ⚠️ **Allocations**: More allocs than JSON (but now far lower B/op)

#### CSV
- ✅ **Fastest processing**: Best marshal/unmarshal performance
- ✅ **Simple format**: Human-readable, widely supported
- ✅ **Low allocations**: Efficient memory usage for unmarshaling
- ❌ **Large size**: 10x larger than SCRT
- ❌ **No type safety**: Everything is strings
- ❌ **High allocations**: 19,912 allocs for 10K marshal operations

#### JSON
- ✅ **Good marshal speed**: Faster than SCRT for small datasets
- ✅ **Human-readable**: Easy debugging
- ✅ **Universal support**: Works everywhere
- ❌ **Large size**: 17x larger than SCRT
- ❌ **Slow unmarshal**: 3.4x slower than SCRT

### 💡 Optimal Use Cases for SCRT
- **Storage-constrained environments**: 94% size reduction vs JSON, 91% vs CSV
- **Network transfer**: Significantly reduced bandwidth costs
- **Read-heavy workloads**: 3.4x faster unmarshaling than JSON
- **Large datasets**: Excellent performance scaling (10K+ records)
- **Type-safe operations**: Built-in schema validation
- **Mixed operations**: Superior round-trip performance vs JSON

### 🎯 When to Use Each

**Use SCRT when:**
- ✅ Storage/bandwidth is expensive or limited
- ✅ Reading data frequently (read-heavy workloads)
- ✅ Working with large datasets (10K+ records)
- ✅ Need type-safe schema validation
- ✅ Performance at scale matters
- ✅ Compression ratio is critical

**Use CSV when:**
- ✅ Maximum processing speed is critical
- ✅ Simple tabular data
- ✅ Need human-readable format
- ✅ Spreadsheet compatibility required
- ✅ Size is not a concern
- ✅ Quick data exchange/import-export

**Use JSON when:**
- ✅ Human readability is critical
- ✅ Debugging/inspection needed frequently
- ✅ Small payloads (< 100 records)
- ✅ Interoperability with non-Go systems is essential
- ✅ Nested/complex data structures
- ✅ Web APIs and standard REST interfaces

## Performance Summary

### Data Size Champion: SCRT 🏆
- **94% smaller** than JSON (17.3x compression)
- **91% smaller** than CSV (11.3x compression)
- Ideal for storage and network transfer

### Speed Champions by Operation:

**Marshaling**: CSV > SCRT ≳ JSON
- CSV: Still wins at 100 & 10K records
- SCRT: New fastest option at 1K records while staying compact
- JSON: Close second for small payloads

**Unmarshaling**: CSV > SCRT (~3.7x faster than JSON) > JSON
- CSV: ~1.4x faster than SCRT, ~5.5x faster than JSON
- SCRT: ~3.8x faster than JSON
- Both significantly outperform JSON

**Round-Trip**: CSV (319µs) > SCRT (377µs) > JSON (1,018µs)
- CSV: Fastest overall
- SCRT: ~2.7x faster than JSON with lower bytes/op
- JSON: Slowest

**Overall**: SCRT provides the **best balance** of size efficiency and performance for read-heavy workloads and storage-critical applications. CSV wins on pure speed but at 11x the storage cost. JSON offers universal compatibility but is slowest and largest.
- ✅ **Columnar storage** enables efficient compression
- ✅ **Schema-aware** for type safety
