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
| 100     | 12,738       | 16,445       | 17,109      | **SCRT** ~1.34x faster than CSV | 17 | 2 | 105 |
| 1,000   | 113,976      | 158,662      | 165,376     | **SCRT** ~1.45x faster than CSV/JSON | 16 | 2 | 1,909 |
| 10,000  | 1,118,386    | 1,602,631    | 1,505,143   | **SCRT** ~1.35x faster than CSV; ~1.43x vs JSON | 29 | 3 | 19,912 |

## Unmarshal Performance (Structs)

| Records | SCRT (ns/op) | JSON (ns/op) | CSV (ns/op) | Winner | SCRT Allocs | JSON Allocs | CSV Allocs |
|---------|--------------|--------------|-------------|--------|-------------|-------------|------------|
| 100     | 15,868       | 90,806       | 16,818      | **SCRT** ~1.06x faster than CSV; ~5.7x than JSON | 21 | 214 | 226 |
| 1,000   | 139,909      | 880,967      | 158,975     | **SCRT** ~1.14x faster than CSV; ~6.3x faster than JSON | 21 | 2,017 | 2,029 |
| 10,000  | 1,593,935    | 9,009,530    | 1,914,231   | **SCRT** ~1.20x faster than CSV; ~5.7x faster than JSON | 31 | 20,025 | 20,036 |

**SCRT now leads unmarshalling across the board, trimming ~6x off JSON decode time while edging out CSV.**

## Round-Trip Performance (1,000 records)

| Format | Time (ns/op) | Memory (B/op) | Allocations | Winner |
|--------|--------------|---------------|-------------|--------|
| SCRT   | 252,693      | 121,618       | 37          | **Fastest overall; 1.29x quicker than CSV and ~4x faster than JSON** |
| JSON   | 1,025,999    | 317,430       | 2,019       | - |
| CSV    | 325,557      | 551,355       | 3,938       | - |

**SCRT is now the fastest round-trip path, beating CSV by ~29% and clearing JSON by ~4x while using the fewest bytes/op.**

## Map Performance (1,000 records)

### Marshal
- **SCRT**: 160,435 ns/op, 22,988 B/op, 16 allocs
- **JSON**: 656,888 ns/op, 491,275 B/op, 11,002 allocs
- **Winner**: SCRT ~4.1x faster; ~21x less memory and ~688x fewer allocs

### Unmarshal
- **SCRT**: 268,803 ns/op, 429,925 B/op, 5,766 allocs
- **JSON**: 1,306,901 ns/op, 601,727 B/op, 21,006 allocs
- **Winner**: SCRT ~4.8x faster with 1.4x less memory and ~3.6x fewer allocs

## Typed Map Performance (1,000 records)

Dedicated schema with only `uint64` fields to exercise the new homogenous fast-paths.

### Marshal
- **SCRT**: 74,701 ns/op, 20,189 B/op, 16 allocs
- **JSON**: 656,888 ns/op, 491,275 B/op, 11,002 allocs (same payload as generic map test)
- **Winner**: SCRT ~8.8x faster with ~24x less memory

### Unmarshal
- **SCRT**: 148,892 ns/op, 299,537 B/op, 2,015 allocs
- **JSON**: 1,306,901 ns/op, 601,727 B/op, 21,006 allocs
- **Winner**: SCRT ~8.8x faster with ~2x less memory and ~10x fewer allocs

## Nested Map Performance (1,000 records w/ metadata stubs)

Each field travels as `{"value": <data>, "meta": {...}}`; decode now preserves the `meta` map when it already exists in the destination slice.

- **Marshal**: 2,188,134 ns/op, 2,897,752 B/op, 33,019 allocs
- **Unmarshal**: 2,930,212 ns/op, 5,824,041 B/op, 47,771 allocs (cost dominated by rebuilding the metadata envelopes)

## Bytes Map Zero-Copy Impact (1,000 records, 512-byte payload)

- **SCRT (copy mode)**: 510,020 ns/op, 1,936,913 B/op, 5,762 allocs
- **SCRT (zero-copy)**: 257,855 ns/op, 912,063 B/op, 3,760 allocs
- **JSON**: 1,306,901 ns/op, 601,727 B/op, 21,006 allocs (binary blobs base64-encoded)

**Zero-copy slices cut decode time in half and drop ~1 MB/op plus ~2k allocations compared to the default safe mode.**

## Key Takeaways

### 🏆 SCRT Wins
1. **Data Size**: 94% smaller than JSON, 91% smaller than CSV (17x and 11x compression)
2. **Struct Marshal Speed**: 1.3–1.4x faster than CSV/JSON without codegen
3. **Unmarshal Speed**: ~5-6x faster than JSON for structs and ahead of CSV at every size
4. **Map Marshal**: ~4.1x faster than JSON with ~21x less memory and ~688x fewer allocs
5. **Map Unmarshal**: ~4.8x faster than JSON with ~1.4x less memory and ~3.6x fewer allocs
6. **Round-trip vs JSON**: ~4x faster while also beating CSV on speed and bytes/op

### 🏆 CSV Wins
1. **Human Readability**: Plain-text output that’s trivial to eyeball or edit
2. **Zero Dependencies**: Built into every platform/toolchain without custom readers

### ⚖️ Trade-offs

#### SCRT
- ✅ **Best compression**: 94% smaller than JSON, 91% smaller than CSV
- ✅ **Fastest marshalling & unmarshalling**: ~1.5x faster than CSV/JSON on writes and ~6x faster than JSON on reads
- ✅ **Type-safe schema**: Built-in validation
- ⚠️ **Allocations**: Struct marshals now down to 17–29 allocs (a fresh page buffer per encode) and under 0.02 allocs/row when unmarshalling

#### CSV
- ✅ **Simple format**: Human-readable, widely supported, trivial tooling
- ✅ **Predictable schema**: Works everywhere without custom libraries
- ❌ **Larger payloads**: 10x larger than SCRT
- ❌ **No type safety**: Everything is strings
- ❌ **High allocations**: 19,912 allocs for 10K marshal operations

#### JSON
- ✅ **Good marshal speed**: Simple structs still serialize quickly
- ✅ **Human-readable**: Easy debugging
- ✅ **Universal support**: Works everywhere
- ❌ **Large size**: 17x larger than SCRT
- ❌ **Slow unmarshal**: ~6x slower than SCRT

### 💡 Optimal Use Cases for SCRT
- **Storage-constrained environments**: 94% size reduction vs JSON, 91% vs CSV
- **Network transfer**: Significantly reduced bandwidth costs
- **Read-heavy workloads**: ~6x faster unmarshaling than JSON and faster than CSV
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
- ✅ Simple tabular data needs to be eyeballed or edited in spreadsheets
- ✅ Need human-readable format with ubiquitous tooling
- ✅ Size is not a concern
- ✅ Quick data exchange/import-export via plain text

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

**Marshaling**: SCRT > CSV > JSON
- SCRT: 1.3–1.4x faster than CSV/JSON across 100–10K records
- CSV: Still competitive but now second place
- JSON: Third place but still convenient for ad-hoc payloads

**Unmarshaling**: SCRT > CSV >> JSON
- SCRT: Beats CSV by 5‑10% and JSON by ~5-6x while using only 21–32 allocs/page
- CSV: Second fastest but 20–80% more memory
- JSON: Trailing far behind

**Round-Trip**: SCRT (253 µs) > CSV (326 µs) >> JSON (1.03 ms)
- SCRT: Overall speed leader and lowest bytes/op (~119 KB vs 551 KB for CSV)
- CSV: Close second but still 3.9k allocs/op
- JSON: Slowest and largest

**Overall**: SCRT now delivers the **best balance**—and the top speed—for both encode/decode while staying ultra-compact. CSV remains handy for hand-edited text but costs ~11x the space, and JSON offers universal compatibility at the expense of throughput and size.
- ✅ **Columnar storage** enables efficient compression
- ✅ **Schema-aware** for type safety
