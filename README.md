# Reef Convert

[![CI](https://github.com/QTSurfer/qtsurfer-reef-convert/actions/workflows/ci.yml/badge.svg)](https://github.com/QTSurfer/qtsurfer-reef-convert/actions/workflows/ci.yml)
[![JitPack](https://jitpack.io/v/QTSurfer/qtsurfer-reef-convert.svg)](https://jitpack.io/#QTSurfer/qtsurfer-reef-convert)
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

This produces a fat JAR at `target/reef-convert-0.1.0.jar`.

### Usage

```
reef-convert <input.parquet> [output.reef] [options]

Options:
  --columns COL:TYPE:CODEC,...   Column mappings (default: auto-detect from schema)
  --inspect                      Show Parquet schema and exit

Types:  long, double, binary
Codecs: delta_varint, alp, raw, varlen, varlen_zstd, varlen_gzip
```

### Auto-detect mode

When no `--columns` are specified, the CLI reads the Parquet schema and maps types automatically:

| Parquet type        | Reef DataType | Reef Codec     |
|---------------------|---------------|----------------|
| INT64, INT32        | LONG          | DELTA_VARINT   |
| DOUBLE, FLOAT       | DOUBLE        | ALP            |
| BINARY, FIXED_LEN   | BINARY        | VARLEN_ZSTD    |
| BOOLEAN             | LONG          | RAW            |

### Examples

```bash
# Auto-detect all columns
java -jar target/reef-convert-0.1.0.jar data.parquet

# Specify output path
java -jar target/reef-convert-0.1.0.jar data.parquet output.reef

# Explicit column mappings
java -jar target/reef-convert-0.1.0.jar data.parquet --columns t:long:delta_varint,cls:double:alp

# Inspect Parquet schema without converting
java -jar target/reef-convert-0.1.0.jar data.parquet --inspect
```

### Example: BTC/USDT tick data

Converting a Parquet file with 3,591 rows of BTC/USDT ticker data (1-second snapshots):

```
$ java -jar target/reef-convert-0.1.0.jar btc_usdt_2026-04-07_18.parquet

  t   → LONG / DELTA_VARINT
  opn → DOUBLE / ALP
  hig → DOUBLE / ALP
  low → DOUBLE / ALP
  cls → DOUBLE / ALP
  vol → DOUBLE / ALP
  vlq → DOUBLE / ALP
  bid → DOUBLE / ALP
  bsz → DOUBLE / ALP
  ask → DOUBLE / ALP
  asz → DOUBLE / ALP

Converted 3,591 rows → btc_usdt_2026-04-07_18.reef (82 KB, 1.4x compression vs parquet)
```

Source Parquet schema:

```
message ticker {
  required int64 t (TIMESTAMP(MILLIS,true));
  required double opn;
  required double hig;
  required double low;
  required double cls;
  required double vol;
  required double vlq;
  required double bid;
  required double bsz;
  required double ask;
  required double asz;
}
```

| Format  | Size   |
|---------|--------|
| Parquet | 118 KB |
| Reef    |  82 KB |

## Java API

```java
var converter = ParquetToReefConverter.builder(new File("ohlcv.parquet"))
    .map("timestamp", DataType.LONG, Codec.DELTA_VARINT)
    .map("open",   DataType.DOUBLE, Codec.ALP)
    .map("high",   DataType.DOUBLE, Codec.ALP)
    .map("low",    DataType.DOUBLE, Codec.ALP)
    .map("close",  DataType.DOUBLE, Codec.ALP)
    .map("volume", DataType.DOUBLE, Codec.ALP)
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
- [reef](https://github.com/QTSurfer/reef-java) 0.5.0
- [parquet-lite](https://github.com/QTSurfer/parquet-lite) 2.0.0

## License

Apache-2.0
