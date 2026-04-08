# Reef Convert

[![CI](https://github.com/QTSurfer/qtsurfer-reef-convert/actions/workflows/ci.yml/badge.svg)](https://github.com/QTSurfer/qtsurfer-reef-convert/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Convert financial time series data to [Reef](https://github.com/QTSurfer/reef-java) format.

## Supported sources

| Source  | Status |
|---------|--------|
| Parquet | ✅ Ready (via [parquet-lite](https://github.com/QTSurfer/parquet-lite)) |
| CSV     | 🔜 Planned |

## CLI

### Build

```bash
mvn package
```

This produces a fat JAR at `target/reef-convert-0.5.0.jar`.

### Usage

```
reef-convert <input.parquet> [output.reef] [options]

Options:
  --columns COL:TYPE:CODEC,...   Column mappings (default: auto-detect from schema)
  --smart                        Auto-select best codec per double column (sample-based, fast)
  --best                         Try all codecs per double column, pick smallest (slower, optimal)
  --inspect                      Show Parquet schema and exit

Types:  long, double, binary
Codecs: delta_varint, alp, gorilla, pongo, raw, varlen, varlen_zstd, varlen_gzip
```

### Codec selection modes

| Mode | Flag | How it works |
|------|------|--------------|
| Default | _(none)_ | Maps Parquet types to codecs (ALP for doubles) |
| Smart | `--smart` | Samples first 512 values per column, trial-encodes with ALP/Gorilla/Pongo, picks smallest |
| Best | `--best` | Trial-encodes all data with every codec per column, picks smallest (optimal) |

With `--smart` or `--best`, each double column shows the comparison:

```
  bid → PONGO [ALP=6.6KB, GORILLA=5.0KB, PONGO=2.8KB*]
```

### Auto-detect type mapping

When no `--columns` are specified, the CLI reads the Parquet schema and maps types automatically:

| Parquet type        | Reef DataType | Reef Codec     |
|---------------------|---------------|----------------|
| INT64, INT32        | LONG          | DELTA_VARINT   |
| DOUBLE, FLOAT       | DOUBLE        | ALP (or auto-selected with `--smart`/`--best`) |
| BINARY, FIXED_LEN   | BINARY        | VARLEN_ZSTD    |
| BOOLEAN             | LONG          | RAW            |

### Examples

```bash
# Auto-detect all columns (ALP for doubles)
java -jar target/reef-convert-0.5.0.jar data.parquet

# Auto-select best codec per column (fast, sample-based)
java -jar target/reef-convert-0.5.0.jar data.parquet --smart

# Optimal codec selection (tries all codecs on all data)
java -jar target/reef-convert-0.5.0.jar data.parquet --best

# Explicit column mappings
java -jar target/reef-convert-0.5.0.jar data.parquet --columns t:long:delta_varint,cls:double:pongo

# Inspect Parquet schema without converting
java -jar target/reef-convert-0.5.0.jar data.parquet --inspect
```

### Benchmarks

Tested on real ticker data (11 columns: timestamp + 10 doubles):

**BTC/USDT** (3,591 rows, 2dp prices ~$65k):

| Mode | Size | vs Parquet (118 KB) |
|------|------|---------------------|
| Default (ALP) | 82 KB | 1.4x |
| `--best` | **73 KB** | **1.6x** |

**ETH/BTC** (2,260 rows, 5dp prices ~0.03):

| Mode | Size | vs Parquet (35 KB) |
|------|------|---------------------|
| Default (ALP) | 35 KB | 1.0x |
| `--best` | **22 KB** | **1.6x** |

**PEPE/USDT** (35,600 rows, 12h of tick data):

| Mode | Size | vs Parquet (753 KB) |
|------|------|---------------------|
| Default (ALP) | 822 KB | 0.9x |
| `--best` | **589 KB** | **1.3x** |

`--best` selects the optimal codec per column — ALP for stable OHLC prices, Pongo for bid/ask with decimal patterns, Gorilla for volatile volumes.

### Example: BTC/USDT with --best

```
$ java -jar target/reef-convert-0.5.0.jar btc_usdt_2026-04-07_18.parquet --best

  t   → DELTA_VARINT
  opn → PONGO  [ALP=6.5KB, GORILLA=11.6KB, PONGO=5.1KB*]
  hig → ALP    [ALP=64B*, GORILLA=461B, PONGO=916B]
  low → ALP    [ALP=64B*, GORILLA=461B, PONGO=916B]
  cls → PONGO  [ALP=6.6KB, GORILLA=11.7KB, PONGO=5.7KB*]
  vol → ALP    [ALP=10.5KB*, GORILLA=21.1KB, PONGO=12.8KB]
  vlq → ALP    [ALP=22.8KB*, GORILLA=23.0KB, PONGO=23.8KB]
  bid → PONGO  [ALP=6.6KB, GORILLA=5.0KB, PONGO=2.8KB*]
  bsz → ALP    [ALP=9.3KB*, GORILLA=28.0KB, PONGO=15.4KB]
  ask → PONGO  [ALP=6.6KB, GORILLA=5.0KB, PONGO=2.9KB*]
  asz → ALP    [ALP=9.8KB*, GORILLA=27.7KB, PONGO=15.7KB]

Converted 3,591 rows → btc_usdt_2026-04-07_18.reef (73 KB, 1.6x compression vs parquet)
```

## Java API

```java
var converter = ParquetToReefConverter.builder(new File("ohlcv.parquet"))
    .map("timestamp", DataType.LONG, Codec.DELTA_VARINT)
    .map("open",   DataType.DOUBLE, Codec.ALP)
    .map("high",   DataType.DOUBLE, Codec.ALP)
    .map("low",    DataType.DOUBLE, Codec.ALP)
    .map("close",  DataType.DOUBLE, Codec.PONGO)
    .map("volume", DataType.DOUBLE, Codec.GORILLA)
    .build();

try (var out = new FileOutputStream("ohlcv.reef")) {
    int rows = converter.convert(out);
}
```

Column renaming is supported:

```java
.map("ts", "timestamp", DataType.LONG, Codec.DELTA_VARINT)
```

## Requirements

- Java 11+
- [reef](https://github.com/QTSurfer/reef-java) 0.6.0
- [parquet-lite](https://github.com/QTSurfer/parquet-lite) 2.0.0

## License

Apache-2.0
