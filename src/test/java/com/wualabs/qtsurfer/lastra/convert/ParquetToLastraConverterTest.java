package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.parquet.ParquetWriter;
import com.wualabs.qtsurfer.lastra.Lastra;
import com.wualabs.qtsurfer.lastra.LastraReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetToLastraConverterTest {

    @TempDir
    Path tempDir;

    @Test
    void convertOhlcvParquetToLastra() throws Exception {
        File parquetFile = tempDir.resolve("test.parquet").toFile();

        org.apache.parquet.schema.MessageType schema = org.apache.parquet.schema.Types.buildMessage()
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("timestamp")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("open")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("high")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("low")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("close")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("volume")
                .named("ohlcv");

        long[] timestamps = {1000L, 2000L, 3000L, 4000L, 5000L};
        double[] opens  = {100.0, 101.5, 102.0, 99.5, 103.0};
        double[] highs  = {102.0, 103.0, 104.5, 101.0, 105.0};
        double[] lows   = {99.0, 100.5, 101.0, 98.0, 102.5};
        double[] closes = {101.5, 102.0, 99.5, 100.5, 104.0};
        double[] vols   = {1000.0, 1500.0, 2000.0, 800.0, 1200.0};

        try (var writer = ParquetWriter.<Object[]>writeFile(schema, parquetFile,
                (record, vw) -> {
                    vw.write("timestamp", record[0]);
                    vw.write("open", record[1]);
                    vw.write("high", record[2]);
                    vw.write("low", record[3]);
                    vw.write("close", record[4]);
                    vw.write("volume", record[5]);
                })) {
            for (int i = 0; i < timestamps.length; i++) {
                writer.write(new Object[]{timestamps[i], opens[i], highs[i], lows[i], closes[i], vols[i]});
            }
        }

        // Convert to Lastra
        var converter = ParquetToLastraConverter.builder(parquetFile)
                .map("timestamp", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT)
                .map("open", Lastra.DataType.DOUBLE, Lastra.Codec.ALP)
                .map("high", Lastra.DataType.DOUBLE, Lastra.Codec.ALP)
                .map("low", Lastra.DataType.DOUBLE, Lastra.Codec.ALP)
                .map("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP)
                .map("volume", Lastra.DataType.DOUBLE, Lastra.Codec.ALP)
                .build();

        ByteArrayOutputStream lastraOut = new ByteArrayOutputStream();
        int rowCount = converter.convert(lastraOut);

        assertThat(rowCount).isEqualTo(5);

        // Verify by reading the Lastra output
        LastraReader reader = LastraReader.from(lastraOut.toByteArray());

        assertThat(reader.seriesRowCount()).isEqualTo(5);
        assertThat(reader.seriesColumns()).hasSize(6);

        assertThat(reader.readSeriesLong("timestamp")).containsExactly(timestamps);
        assertThat(reader.readSeriesDouble("close")).containsExactly(closes);
        assertThat(reader.readSeriesDouble("volume")).containsExactly(vols);
    }

    @Test
    void filtersRowsByPredicateOnExtraColumn() throws Exception {
        // Source parquet carries two series interleaved (mkt = "binance" rows + mkt = "bybit"
        // rows). The filter should keep only the binance rows in the Lastra output, while `mkt`
        // itself should NOT appear as a Lastra column (it's declared as a filter-only column).
        File parquetFile = tempDir.resolve("multi.parquet").toFile();

        org.apache.parquet.schema.MessageType schema = org.apache.parquet.schema.Types.buildMessage()
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
                .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType()).named("mkt")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("ts")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("close")
                .named("ohlcv");

        Object[][] rows = new Object[][] {
                {"binance", 1000L, 100.0},
                {"bybit",   1000L, 200.0},
                {"binance", 2000L, 101.0},
                {"bybit",   2000L, 201.0},
                {"binance", 3000L, 102.0}
        };

        try (var writer = ParquetWriter.<Object[]>writeFile(schema, parquetFile,
                (record, vw) -> {
                    vw.write("mkt", record[0]);
                    vw.write("ts", record[1]);
                    vw.write("close", record[2]);
                })) {
            for (Object[] row : rows) {
                writer.write(row);
            }
        }

        var converter = ParquetToLastraConverter.builder(parquetFile)
                .map("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT)
                .map("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP)
                .filter(row -> "binance".equals(row.get("mkt")), "mkt")
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int rowCount = converter.convert(out);

        // 3 binance rows survive the filter, 2 bybit rows are dropped.
        assertThat(rowCount).isEqualTo(3);

        LastraReader reader = LastraReader.from(out.toByteArray());
        assertThat(reader.seriesRowCount()).isEqualTo(3);
        // Only the two mapped columns reach the output; mkt is filter-only.
        assertThat(reader.seriesColumns()).hasSize(2);
        assertThat(reader.seriesColumns().stream().map(c -> c.name())).containsExactly("ts", "close");

        assertThat(reader.readSeriesLong("ts")).containsExactly(1000L, 2000L, 3000L);
        assertThat(reader.readSeriesDouble("close")).containsExactly(100.0, 101.0, 102.0);
    }

    @Test
    void transformAggregatesRowsBeforeEncode() throws Exception {
        // Source: 6 rows of (ts, close). Transform: pair them up (3 buckets of 2), keep last
        // close + last ts per bucket, sum-style. Output: 3 Lastra rows.
        File parquetFile = tempDir.resolve("transform.parquet").toFile();

        org.apache.parquet.schema.MessageType schema = org.apache.parquet.schema.Types.buildMessage()
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("ts")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("close")
                .named("ohlcv");

        long[] tss = {1L, 2L, 3L, 4L, 5L, 6L};
        double[] cls = {10.0, 11.0, 12.0, 13.0, 14.0, 15.0};

        try (var writer = ParquetWriter.<Object[]>writeFile(schema, parquetFile,
                (record, vw) -> {
                    vw.write("ts", record[0]);
                    vw.write("close", record[1]);
                })) {
            for (int i = 0; i < tss.length; i++) {
                writer.write(new Object[]{tss[i], cls[i]});
            }
        }

        var converter = ParquetToLastraConverter.builder(parquetFile)
                .map("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT)
                .map("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP)
                .transform(rows -> {
                    java.util.List<java.util.Map<String, Object>> out = new java.util.ArrayList<>();
                    for (int i = 0; i < rows.size(); i += 2) {
                        int end = Math.min(i + 2, rows.size());
                        java.util.Map<String, Object> r = new java.util.LinkedHashMap<>();
                        r.put("ts", rows.get(end - 1).get("ts"));
                        r.put("close", rows.get(end - 1).get("close"));
                        out.add(r);
                    }
                    return out;
                })
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int rowCount = converter.convert(out);
        assertThat(rowCount).isEqualTo(3);

        LastraReader reader = LastraReader.from(out.toByteArray());
        assertThat(reader.seriesRowCount()).isEqualTo(3);
        // Bucket pairs: (1,2)→last=2, (3,4)→4, (5,6)→6.
        assertThat(reader.readSeriesLong("ts")).containsExactly(2L, 4L, 6L);
        assertThat(reader.readSeriesDouble("close")).containsExactly(11.0, 13.0, 15.0);
    }

    @Test
    void transformReturningEmptyTreatsLikeEmptySource() throws Exception {
        File parquetFile = tempDir.resolve("transform-empty.parquet").toFile();
        org.apache.parquet.schema.MessageType schema = org.apache.parquet.schema.Types.buildMessage()
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("ts")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("close")
                .named("ohlcv");

        try (var writer = ParquetWriter.<Object[]>writeFile(schema, parquetFile,
                (record, vw) -> {
                    vw.write("ts", record[0]);
                    vw.write("close", record[1]);
                })) {
            writer.write(new Object[]{1L, 1.0});
            writer.write(new Object[]{2L, 2.0});
        }

        var converter = ParquetToLastraConverter.builder(parquetFile)
                .map("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT)
                .map("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP)
                .transform(rows -> java.util.Collections.emptyList())
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThat(converter.convert(out)).isZero();
    }

    @Test
    void filterMatchingZeroRowsReturnsZero() throws Exception {
        File parquetFile = tempDir.resolve("nomatch.parquet").toFile();

        org.apache.parquet.schema.MessageType schema = org.apache.parquet.schema.Types.buildMessage()
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
                .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType()).named("mkt")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("ts")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("close")
                .named("ohlcv");

        try (var writer = ParquetWriter.<Object[]>writeFile(schema, parquetFile,
                (record, vw) -> {
                    vw.write("mkt", record[0]);
                    vw.write("ts", record[1]);
                    vw.write("close", record[2]);
                })) {
            writer.write(new Object[]{"binance", 1000L, 100.0});
            writer.write(new Object[]{"bybit",   1000L, 200.0});
        }

        var converter = ParquetToLastraConverter.builder(parquetFile)
                .map("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT)
                .map("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP)
                .filter(row -> "kraken".equals(row.get("mkt")), "mkt")
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThat(converter.convert(out)).isZero();
    }
}
