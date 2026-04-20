# Lastra Convert

[![CI](https://github.com/QTSurfer/lastra-convert/actions/workflows/ci.yml/badge.svg)](https://github.com/QTSurfer/lastra-convert/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Bidirectional converter between [Lastra](https://github.com/QTSurfer/lastra-java), [Apache Parquet](https://parquet.apache.org/), and CSV formats for time series data.

## Supported conversions

| Source → Target | Status |
|-----------------|--------|
| Parquet → Lastra | ✅ Ready (auto-detect, --smart, --best) |
| Lastra → Parquet | ✅ Ready (ZSTD compressed, lossless roundtrip) |
| CSV → Lastra | ✅ Ready (auto-detect types and delimiter) |
| Lastra → CSV | ✅ Ready (plain decimal output) |

## CLI

### Build

```bash
mvn package
```

This produces a fat JAR at `target/lastra-convert-0.10.5.jar`.

### Usage

```
lastra-convert <input> [output] [options]

Formats (auto-detected by extension):
  .parquet/.pqt → Lastra     .csv/.tsv → Lastra
  .lastra → Parquet           .lastra → CSV (if output is .csv)

Options:
  --columns COL:TYPE:CODEC,...   Column mappings (Parquet/CSV→Lastra only)
  --smart                        Auto-select best codec per column (sample-based, fast)
  --best                         Try all codecs per column, pick smallest (slower, optimal)
  --inspect                      Show file structure and exit (Parquet and Lastra)

Types:  long, double, binary
Codecs: delta_varint, alp, gorilla, pongo, raw, varlen, varlen_zstd, varlen_gzip
```

### Parquet → Lastra

```bash
# Auto-detect all columns (ALP for doubles)
java -jar target/lastra-convert-0.10.5.jar data.parquet

# Auto-select best codec per column (fast, sample-based)
java -jar target/lastra-convert-0.10.5.jar data.parquet --smart

# Optimal codec selection (tries all codecs on all data)
java -jar target/lastra-convert-0.10.5.jar data.parquet --best

# Explicit column mappings
java -jar target/lastra-convert-0.10.5.jar data.parquet --columns t:long:delta_varint,cls:double:pongo
```

### CSV → Lastra

```bash
# Auto-detect types from first data row
java -jar target/lastra-convert-0.10.5.jar data.csv

# Supports comma, tab, and semicolon delimiters (auto-detected)
java -jar target/lastra-convert-0.10.5.jar data.tsv
```

CSV type detection:
- Integer values → LONG / DELTA_VARINT
- Decimal values → DOUBLE / ALP
- Everything else → BINARY / VARLEN_ZSTD

### Lastra → Parquet

```bash
java -jar target/lastra-convert-0.10.5.jar data.lastra

# Explicit output path
java -jar target/lastra-convert-0.10.5.jar data.lastra output.parquet
```

### Lastra → CSV

```bash
java -jar target/lastra-convert-0.10.5.jar data.lastra output.csv
```

### Inspect

```bash
# Parquet schema
java -jar target/lastra-convert-0.10.5.jar data.parquet --inspect

# Lastra structure
java -jar target/lastra-convert-0.10.5.jar data.lastra --inspect
```

```
Lastra file: btc_usdt.lastra
  Series: 3,591 rows, 11 columns
    t            LONG / DELTA_VARINT
    opn          DOUBLE / PONGO
    hig          DOUBLE / ALP
    low          DOUBLE / ALP
    cls          DOUBLE / PONGO
    vol          DOUBLE / ALP
    vlq          DOUBLE / ALP
    bid          DOUBLE / PONGO
    bsz          DOUBLE / ALP
    ask          DOUBLE / PONGO
    asz          DOUBLE / ALP
```

### Codec selection modes (Parquet/CSV → Lastra)

| Mode | Flag | How it works |
|------|------|--------------|
| Default | _(none)_ | Maps types to codecs (ALP for doubles) |
| Smart | `--smart` | Samples first 512 values per column, trial-encodes, picks smallest |
| Best | `--best` | Trial-encodes all data with every codec, picks smallest (optimal) |

With `--smart` or `--best`, each double column shows the comparison:

```
  bid → PONGO [ALP=6.6KB, GORILLA=5.0KB, PONGO=2.8KB*]
```

## Benchmarks

Tested on real ticker data (11 columns: timestamp + 10 doubles):

**BTC/USDT** (3,591 rows, 2dp prices ~$65k):

| Format | Size | Ratio |
|--------|------|-------|
| CSV | 12 KB (100 rows) | 1x |
| Parquet (ZSTD) | 118 KB | — |
| Lastra (ALP default) | 82 KB | 1.4x vs Parquet |
| **Lastra (--best)** | **73 KB** | **1.6x vs Parquet** |
| Roundtrip Parquet | 118 KB | lossless ✓ |
| Roundtrip CSV | 12 KB | lossless ✓ |

**ETH/BTC** (2,260 rows, 5dp prices ~0.03):

| Format | Size | Ratio |
|--------|------|-------|
| Parquet (ZSTD) | 35 KB | 1x |
| **Lastra (--best)** | **22 KB** | **1.6x** |

**PEPE/USDT** (35,600 rows, 12h of tick data):

| Format | Size | Ratio |
|--------|------|-------|
| Parquet (ZSTD) | 753 KB | 1x |
| **Lastra (--best)** | **589 KB** | **1.3x** |

### Example: BTC/USDT with --best

```
$ java -jar target/lastra-convert-0.10.5.jar btc_usdt.parquet --best

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

Converted 3,591 rows → btc_usdt.lastra (73 KB, 1.6x compression vs parquet)
```

## Java API

### Parquet → Lastra

```java
var converter = ParquetToLastraConverter.builder(new File("ohlcv.parquet"))
    .map("timestamp", DataType.LONG, Codec.DELTA_VARINT)
    .map("open",   DataType.DOUBLE, Codec.ALP)
    .map("close",  DataType.DOUBLE, Codec.PONGO)
    .map("volume", DataType.DOUBLE, Codec.ALP)
    .build();

try (var out = new FileOutputStream("ohlcv.lastra")) {
    int rows = converter.convert(out);
}
```

### CSV → Lastra

```java
var converter = new CsvToLastraConverter(new File("data.csv"));

try (var out = new FileOutputStream("data.lastra")) {
    int rows = converter.convert(out);
}
```

### Lastra → Parquet

```java
var converter = new LastraToParquetConverter(new File("ohlcv.lastra"));

try (var out = new FileOutputStream("ohlcv.parquet")) {
    int rows = converter.convert(out);
}
```

### Lastra → CSV

```java
var converter = new LastraToCsvConverter(new File("data.lastra"));

try (var out = new FileOutputStream("data.csv")) {
    int rows = converter.convert(out);
}
```

### Inspect

```java
LastraToParquetConverter.inspect(new File("data.lastra"));
```

## Requirements

- Java 11+
- [lastra-java](https://github.com/QTSurfer/lastra-java)
- [parquet-lite](https://github.com/QTSurfer/parquet-lite)

## License

Apache-2.0
