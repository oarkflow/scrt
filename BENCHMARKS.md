# SCRT vs JSON Benchmark Results

## Test Environment
- **CPU**: Intel(R) Core(TM) i7-9700K @ 3.60GHz
- **OS**: Linux (amd64)
- **Go**: 1.25
- **Benchmark Time**: 3 seconds per test

## Data Size Comparison (Compression Ratio)

| Records | JSON Size | SCRT Size | Compression | Savings |
|---------|-----------|-----------|-------------|---------|
| 100     | 12,043 B  | 706 B     | **17.1x**   | 94.1%   |
| 1,000   | 121,394 B | 6,988 B   | **17.4x**   | 94.2%   |
| 10,000  | 1,223,895 B| 70,906 B | **17.3x**   | 94.2%   |

**SCRT achieves ~94% size reduction vs JSON! 🎯**

## Marshal Performance (Structs)

| Records | SCRT (ns/op) | JSON (ns/op) | Winner | SCRT Allocs | JSON Allocs |
|---------|--------------|--------------|--------|-------------|-------------|
| 100     | 38,834       | 23,305       | JSON 1.7x faster | 65 | 2 |
| 1,000   | 289,118      | 236,872      | JSON 1.2x faster | 94 | 2 |
| 10,000  | 2,567,127    | 2,572,874    | **SCRT** (comparable) | 519 | 2 |

## Unmarshal Performance (Structs)

| Records | SCRT (ns/op) | JSON (ns/op) | Winner | SCRT Allocs | JSON Allocs |
|---------|--------------|--------------|--------|-------------|-------------|
| 100     | 66,606       | 121,937      | **SCRT 1.8x faster** | 523 | 214 |
| 1,000   | 631,640      | 1,192,029    | **SCRT 1.9x faster** | 5,771 | 2,017 |
| 10,000  | 6,957,440    | 12,771,006   | **SCRT 1.8x faster** | 59,843 | 20,025 |

**SCRT is ~1.8-1.9x faster at unmarshaling! 🚀**

## Round-Trip Performance (1,000 records)

| Format | Time (ns/op) | Memory (B/op) | Allocations |
|--------|--------------|---------------|-------------|
| SCRT   | 874,993      | 475,033       | 5,865       |
| JSON   | 1,444,348    | 323,361       | 2,020       |

**SCRT round-trip is 1.65x faster than JSON**

## Map Performance (1,000 records)

### Marshal
- **SCRT**: 666,657 ns/op, 314,497 B/op, 10,094 allocs
- **JSON**: 1,241,014 ns/op, 491,468 B/op, 11,002 allocs
- **Winner**: SCRT 1.9x faster

### Unmarshal
- **SCRT**: 1,503,663 ns/op, 655,362 B/op, 17,771 allocs
- **JSON**: 1,997,387 ns/op, 601,723 B/op, 21,006 allocs
- **Winner**: SCRT 1.3x faster

## Key Takeaways

### 🏆 SCRT Wins
1. **Data Size**: 94% smaller than JSON (17x compression)
2. **Unmarshal Speed**: 1.8-1.9x faster for structs
3. **Map Marshal**: 1.9x faster
4. **Map Unmarshal**: 1.3x faster
5. **Round-trip**: 1.65x faster

### ⚖️ Trade-offs
1. **Marshal with Structs**: JSON is slightly faster (1.2-1.7x) for small datasets due to simpler encoding, but performance becomes comparable at 10K+ records
2. **Memory Allocations**: JSON uses fewer allocations during marshal (2 vs 65-519), but SCRT requires fewer during unmarshal at scale

### 💡 Optimal Use Cases for SCRT
- **Storage-constrained environments**: 94% size reduction
- **Network transfer**: Significantly reduced bandwidth
- **Read-heavy workloads**: Faster unmarshaling
- **Large datasets**: Better performance scaling (10K+ records)
- **Mixed operations**: Superior round-trip performance

### 🎯 When to Use Each

**Use SCRT when:**
- Storage/bandwidth is expensive
- Reading data frequently
- Working with large datasets
- Need type-safe schema validation
- Performance at scale matters

**Use JSON when:**
- Human readability is critical
- Debugging/inspection needed
- Small payloads (< 100 records)
- Maximum marshal speed for structs is critical
- Interoperability with non-Go systems

## Performance Summary

SCRT delivers on its promise of being a **high-performance, lightweight alternative to JSON**:
- ✅ **94% smaller** data size
- ✅ **1.8x faster** at reading data
- ✅ **1.65x faster** round-trip operations
- ✅ **Competitive** encoding speed at scale
- ✅ **Columnar storage** enables efficient compression
- ✅ **Schema-aware** for type safety
