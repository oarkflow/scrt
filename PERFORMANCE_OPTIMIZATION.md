# SCRT Performance Optimization Roadmap

## Current Performance Analysis

Based on benchmark results (Feb 6, 2026), SCRT has closed the gap significantly with Protocol Buffers:

- **Marshal Performance Gap**: Proto is ~1.7x faster (6,045 ns/op vs 10,206 ns/op for 100 items). **Improved from 2.4x gap.**
- **Unmarshal Performance Gap**: Proto is ~2.7x faster (7,920 ns/op vs 21,867 ns/op for 100 items).
- **Memory Efficiency**: SCRT uses fewer allocations but reflection overhead is significant

## Key Performance Bottlenecks Identified

### 1. **Heavy Reflection Usage** 🚨 *HIGH IMPACT*
**Current Issue**: Extensive use of `reflect.ValueOf()`, `reflect.TypeOf()`, and runtime type inspection
**Impact**: ~60-70% of marshal/unmarshal time spent in reflection
**Location**: `marshal.go:populateRowFromStruct()`, `unmarshal.go:assignRowToStruct()`

### 2. **Columnar Processing Overhead** 🚨 *HIGH IMPACT*
**Current Issue**: Row → Column → Page transformation adds multiple data copies
**Impact**: Additional memory allocations and CPU cycles
**Location**: `codec/writer.go`, `page/builder.go`

### 3. **Schema Validation at Runtime** ⚠️ *MEDIUM IMPACT*
**Current Issue**: Schema validation happens during every marshal/unmarshal operation
**Impact**: CPU overhead for field lookups and type checking
**Location**: `schema/resolve.go`, `marshal.go:populateRow()`

### 4. **Buffer Management** ⚠️ *MEDIUM IMPACT*
**Current Issue**: Multiple buffer allocations and copies in the encoding pipeline
**Impact**: Memory allocation pressure
**Location**: `page/builder.go`, `codec/writer.go`

## Optimization Strategies

### Phase 1: Code Generation (High Impact, High Effort)

#### 1.1 **Schema-Specific Code Generation**
```go
// Generate marshal/unmarshal functions for specific schemas
// Instead of: populateRowFromStruct(row, value, schema)
// Generate: marshalMessageStruct(row, msg *MessageStruct)
func generateSchemaCode(schema *schema.Schema) string {
    // Generate type-specific marshal/unmarshal functions
    // Eliminate reflection for known struct types
}
```

**Expected Improvement**: 40-60% performance gain
**Effort**: High (code generation infrastructure)
**Risk**: Increased binary size, complexity

#### 1.2 **Field Accessor Generation**
```go
// Generate direct field access instead of reflection
type MessageMarshaller struct {
    msgIDOffset   uintptr
    userOffset    uintptr
    textOffset    uintptr
    // ...
}

func (m *MessageMarshaller) Marshal(row *codec.Row, msg *MessageStruct) {
    row.SetUint64(0, *(*uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(msg)) + m.msgIDOffset)))
    // Direct memory access, no reflection
}
```

**Expected Improvement**: 30-50% performance gain
**Effort**: Medium-High
**Risk**: Unsafe pointer usage, type safety concerns

### Phase 2: Algorithmic Optimizations (Medium-High Impact, Medium Effort)

#### 2.1 **Direct Row Encoding** ✅ *COMPLETED*
**Status**: Implemented & Verified (Feb 6, 2026)
**Result**: ~28-35% performance gain achieved.
**Implementation**: Refactored `codec.Writer` to support direct value appending and updated `marshal.go` to use cached struct bindings.

**Current**: Struct → Row → Column → Page → Buffer
**Optimized**: Struct → Direct Page Encoding

```go
func encodeIntoDirect(dst *bytes.Buffer, s *schema.Schema, input any) error {
    // Skip Row intermediate representation
    // Encode directly from struct to page format
    writer := codec.NewWriter(dst, s, 1024)
    // Direct field-to-column encoding
}
```

**Expected Improvement**: 20-30% performance gain
**Actual Improvement**: ~28-35%

#### 2.2 **SIMD-Accelerated Encoding** 🚀 *HIGH PRIORITY*
```go
// Use SIMD for bulk data operations
func encodeUint64ColumnSIMD(values []uint64, dst []byte) int {
    // AVX-512 or NEON instructions for bulk varint encoding
    // Process multiple values simultaneously
}
```

**Expected Improvement**: 15-25% performance gain for large datasets
**Effort**: High (assembly/SIMD intrinsics)
**Risk**: Platform-specific code

#### 2.3 **Zero-Copy Field Mapping**
```go
// Pre-compute field offsets and types at schema load time
type FieldMapping struct {
    offset    uintptr
    kind      reflect.Kind
    converter func([]byte, unsafe.Pointer) // Direct conversion functions
}

func buildFieldMappings(schema *schema.Schema, structType reflect.Type) []FieldMapping {
    // Pre-compute all field access patterns
}
```

**Expected Improvement**: 25-35% performance gain
**Effort**: Medium
**Risk**: Low

### Phase 3: Memory Optimizations (Medium Impact, Low-Medium Effort)

#### 3.1 **Arena-Based Allocation** ⚡ *QUICK WIN*
```go
// Use arena allocation for temporary objects
type EncodingArena struct {
    rows    []codec.Row
    buffers []bytes.Buffer
    // Pre-allocated objects
}

func (a *EncodingArena) AcquireRow() *codec.Row {
    // Return pre-allocated row from arena
}
```

**Expected Improvement**: 10-15% performance gain, reduced GC pressure
**Effort**: Low-Medium
**Risk**: Low

#### 3.2 **Buffer Reuse Optimization**
```go
// Smarter buffer pooling with size classes
var bufferPools [8]sync.Pool // Different size classes

func getBuffer(sizeHint int) *bytes.Buffer {
    sizeClass := sizeHint / 1024 // 1KB, 2KB, 4KB, etc.
    if sizeClass >= len(bufferPools) {
        sizeClass = len(bufferPools) - 1
    }
    return bufferPools[sizeClass].Get().(*bytes.Buffer)
}
```

**Expected Improvement**: 5-10% performance gain
**Effort**: Low
**Risk**: Low

### Phase 4: Architectural Improvements (Medium Impact, Medium Effort)

#### 4.1 **Streaming Pipeline Optimization**
```go
// Pipeline: Input → Validate → Encode → Compress → Output
type EncodingPipeline struct {
    validator *fastjson.Validator  // Pre-compiled schema validator
    encoder   *SIMDEncoder         // SIMD-accelerated encoder
    compressor *LZ4Compressor      // Optional compression
}

func (p *EncodingPipeline) Process(input any) ([]byte, error) {
    // Streamlined processing pipeline
}
```

**Expected Improvement**: 15-20% performance gain
**Effort**: Medium
**Risk**: Medium

#### 4.2 **CPU Cache Optimization**
```go
// Optimize data layout for cache efficiency
type CacheOptimizedPage struct {
    // Group frequently accessed fields together
    presenceBits []uint64    // Hot path
    rowIndexes   []int32     // Hot path
    data         []byte      // Cold path
}
```

**Expected Improvement**: 5-15% performance gain
**Effort**: Medium
**Risk**: Low

## Implementation Priority

### Immediate (Next Sprint) - Quick Wins
1. **Direct Row Encoding** (Phase 2.1) - 20-30% gain
2. **Arena-Based Allocation** (Phase 3.1) - 10-15% gain
3. **Buffer Reuse Optimization** (Phase 3.2) - 5-10% gain

### Short Term (1-2 Months) - High Impact
1. **Zero-Copy Field Mapping** (Phase 2.3) - 25-35% gain
2. **Schema-Specific Code Generation** (Phase 1.1) - 40-60% gain

### Long Term (2-6 Months) - Advanced
1. **SIMD-Accelerated Encoding** (Phase 2.2) - 15-25% gain
2. **Field Accessor Generation** (Phase 1.2) - 30-50% gain

## Performance Targets

| Metric | Previous (Jan 6) | Current (Feb 6) | Target (Phase 2 Full) | Target (Final) |
|--------|------------------|-----------------|-----------------------|----------------|
| Marshal (100 items) | 14,189 ns/op | **10,206 ns/op** | 7,000 ns/op | 5,000 ns/op |
| Unmarshal (100 items) | 20,314 ns/op | 21,867 ns/op | 10,000 ns/op | 7,000 ns/op |
| Size Ratio vs JSON | 17.8x | 17.8x | 17.8x | 17.8x |
| Allocations (100 items) | 15 allocs/op | **12 allocs/op** | 8 allocs/op | 5 allocs/op |

## Success Metrics

- **Performance**: Match or exceed Protocol Buffers speed while maintaining size advantage
- **Memory**: Reduce allocations by 50-70%
- **Maintainability**: Keep code complexity manageable
- **Compatibility**: Maintain backward compatibility with existing SCRT data

## Risk Mitigation

1. **Incremental Implementation**: Roll out optimizations gradually with benchmarks
2. **Fallback Mechanisms**: Keep reflection-based code as fallback
3. **Comprehensive Testing**: Extensive testing for edge cases and compatibility
4. **Performance Regression Testing**: Automated benchmarks in CI/CD

## Conclusion

SCRT can achieve Protocol Buffers-level performance while maintaining its significant size advantages through:

1. **Eliminating reflection overhead** via code generation and direct field access
2. **Optimizing the encoding pipeline** with direct encoding and SIMD acceleration
3. **Improving memory management** with arenas and better buffer pooling
4. **Architectural improvements** for cache efficiency and streaming processing

The roadmap prioritizes high-impact, low-risk optimizations first, with more advanced techniques for long-term performance gains.
