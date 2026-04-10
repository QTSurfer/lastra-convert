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
}
