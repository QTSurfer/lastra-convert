# Reef Convert

[![CI](https://github.com/QTSurfer/qtsurfer-reef-convert/actions/workflows/ci.yml/badge.svg)](https://github.com/QTSurfer/qtsurfer-reef-convert/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Bidirectional converter between [Reef](https://github.com/QTSurfer/reef-java), [Apache Parquet](https://parquet.apache.org/), and CSV formats for time series data.

## Supported conversions

| Source → Target | Status |
|-----------------|--------|
| Parquet → Reef | ✅ Ready (auto-detect, --smart, --best) |
| Reef → Parquet | ✅ Ready (ZSTD compressed, lossless roundtrip) |
| CSV → Reef | ✅ Ready (auto-detect types and delimiter) |
| Reef → CSV | ✅ Ready (plain decimal output) |

## CLI

### Build

```bash
mvn package
```

This produces a fat JAR at `target/reef-convert-0.7.0.jar`.

### Usage

```
reef-convert <input> [output] [options]

Formats (auto-detected by extension):
  .parquet/.pqt → Reef     .csv/.tsv → Reef
  .reef → Parquet           .reef → CSV (if output is .csv)

Options:
  --columns COL:TYPE:CODEC,...   Column mappings (Parquet/CSV→Reef only)
  --smart                        Auto-select best codec per column (sample-based, fast)
  --best                         Try all codecs per column, pick smallest (slower, optimal)
  --inspect                      Show file structure and exit (Parquet and Reef)

Types:  long, double, binary
Codecs: delta_varint, alp, gorilla, pongo, raw, varlen, varlen_zstd, varlen_gzip
```

### Parquet → Reef

```bash
# Auto-detect all columns (ALP for doubles)
java -jar target/reef-convert-0.7.0.jar data.parquet

# Auto-select best codec per column (fast, sample-based)
java -jar target/reef-convert-0.7.0.jar data.parquet --smart

# Optimal codec selection (tries all codecs on all data)
java -jar target/reef-convert-0.7.0.jar data.parquet --best

# Explicit column mappings
java -jar target/reef-convert-0.7.0.jar data.parquet --columns t:long:delta_varint,cls:double:pongo
```

### CSV → Reef

```bash
# Auto-detect types from first data row
java -jar target/reef-convert-0.7.0.jar data.csv

# Supports comma, tab, and semicolon delimiters (auto-detected)
java -jar target/reef-convert-0.7.0.jar data.tsv
```

CSV type detection:
- Integer values → LONG / DELTA_VARINT
- Decimal values → DOUBLE / ALP
- Everything else → BINARY / VARLEN_ZSTD

### Reef → Parquet

```bash
java -jar target/reef-convert-0.7.0.jar data.reef

# Explicit output path
java -jar target/reef-convert-0.7.0.jar data.reef output.parquet
```

### Reef → CSV

```bash
java -jar target/reef-convert-0.7.0.jar data.reef output.csv
```

### Inspect

```bash
# Parquet schema
java -jar target/reef-convert-0.7.0.jar data.parquet --inspect

# Reef structure
java -jar target/reef-convert-0.7.0.jar data.reef --inspect
```

```
Reef file: btc_usdt.reef
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

### Codec selection modes (Parquet/CSV → Reef)

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
| Reef (ALP default) | 82 KB | 1.4x vs Parquet |
| **Reef (--best)** | **73 KB** | **1.6x vs Parquet** |
| Roundtrip Parquet | 118 KB | lossless ✓ |
| Roundtrip CSV | 12 KB | lossless ✓ |

**ETH/BTC** (2,260 rows, 5dp prices ~0.03):

| Format | Size | Ratio |
|--------|------|-------|
| Parquet (ZSTD) | 35 KB | 1x |
| **Reef (--best)** | **22 KB** | **1.6x** |

**PEPE/USDT** (35,600 rows, 12h of tick data):

| Format | Size | Ratio |
|--------|------|-------|
| Parquet (ZSTD) | 753 KB | 1x |
| **Reef (--best)** | **589 KB** | **1.3x** |

### Example: BTC/USDT with --best

```
$ java -jar target/reef-convert-0.7.0.jar btc_usdt.parquet --best

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

Converted 3,591 rows → btc_usdt.reef (73 KB, 1.6x compression vs parquet)
```

## Java API

### Parquet → Reef

```java
var converter = ParquetToReefConverter.builder(new File("ohlcv.parquet"))
    .map("timestamp", DataType.LONG, Codec.DELTA_VARINT)
    .map("open",   DataType.DOUBLE, Codec.ALP)
    .map("close",  DataType.DOUBLE, Codec.PONGO)
    .map("volume", DataType.DOUBLE, Codec.ALP)
    .build();

try (var out = new FileOutputStream("ohlcv.reef")) {
    int rows = converter.convert(out);
}
```

### CSV → Reef

```java
var converter = new CsvToReefConverter(new File("data.csv"));

try (var out = new FileOutputStream("data.reef")) {
    int rows = converter.convert(out);
}
```

### Reef → Parquet

```java
var converter = new ReefToParquetConverter(new File("ohlcv.reef"));

try (var out = new FileOutputStream("ohlcv.parquet")) {
    int rows = converter.convert(out);
}
```

### Reef → CSV

```java
var converter = new ReefToCsvConverter(new File("data.reef"));

try (var out = new FileOutputStream("data.csv")) {
    int rows = converter.convert(out);
}
```

### Inspect

```java
ReefToParquetConverter.inspect(new File("data.reef"));
```

## Requirements

- Java 11+
- [reef](https://github.com/QTSurfer/reef-java) 0.6.1
- [parquet-lite](https://github.com/QTSurfer/parquet-lite) 2.0.0

## License

Apache-2.0
