# Reef Convert

Convert financial time series data to [Reef](https://github.com/QTSurfer/reef-java) format.

## Supported sources

| Source  | Status |
|---------|--------|
| Parquet | ✅ Ready (via [parquet-lite](https://github.com/QTSurfer/parquet-lite)) |
| CSV     | 🔜 Planned |

## Usage

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

## Build

```bash
mvn package
```

## Requirements

- Java 11+
- [reef](https://github.com/QTSurfer/reef-java) 0.5.0
- [parquet-lite](https://github.com/QTSurfer/parquet-lite) 2.0.0

## License

Apache-2.0
