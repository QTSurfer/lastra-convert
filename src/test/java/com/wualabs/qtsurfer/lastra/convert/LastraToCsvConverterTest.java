package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.parquet.ParquetWriter;
import com.wualabs.qtsurfer.lastra.Lastra;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class LastraToCsvConverterTest {

    @TempDir
    Path tempDir;

    @Test
    void writesFullPrecisionDoublesForOrdinaryRows() throws Exception {
        File lastraFile = tempDir.resolve("ordinary.lastra").toFile();
        buildLastra(lastraFile, new long[] {1000L, 2000L}, new double[] {101.5, 99.0});

        String csv = convertToCsv(lastraFile);

        assertThat(csv).isEqualTo("timestamp,close\n1000,101.5\n2000,99\n");
    }

    @Test
    void naNDoubleColumnBecomesBlank_insteadOfCrashing() throws Exception {
        // A close-only ticker row (e.g. a raw on-chain trade with no open/high/low) legitimately
        // has NaN in an unpopulated DOUBLE column -- must export as an empty field, not throw.
        File lastraFile = tempDir.resolve("with-nan.lastra").toFile();
        buildLastra(lastraFile, new long[] {1000L, 2000L}, new double[] {Double.NaN, 99.0});

        String csv = convertToCsv(lastraFile);

        assertThat(csv).isEqualTo("timestamp,close\n1000,\n2000,99\n");
    }

    @Test
    void infiniteDoubleColumnBecomesBlank_insteadOfCrashing() throws Exception {
        File lastraFile = tempDir.resolve("with-inf.lastra").toFile();
        buildLastra(
                lastraFile,
                new long[] {1000L, 2000L},
                new double[] {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY});

        String csv = convertToCsv(lastraFile);

        assertThat(csv).isEqualTo("timestamp,close\n1000,\n2000,\n");
    }

    private void buildLastra(File lastraFile, long[] timestamps, double[] closes) throws Exception {
        File parquetFile = tempDir.resolve(lastraFile.getName() + ".parquet").toFile();

        org.apache.parquet.schema.MessageType schema = org.apache.parquet.schema.Types.buildMessage()
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("timestamp")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("close")
                .named("ticker");

        try (var writer = ParquetWriter.<Object[]>writeFile(schema, parquetFile,
                (record, vw) -> {
                    vw.write("timestamp", record[0]);
                    vw.write("close", record[1]);
                })) {
            for (int i = 0; i < timestamps.length; i++) {
                writer.write(new Object[] {timestamps[i], closes[i]});
            }
        }

        var converter = ParquetToLastraConverter.builder(parquetFile)
                .map("timestamp", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT)
                .map("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP)
                .build();

        try (var out = new java.io.FileOutputStream(lastraFile)) {
            converter.convert(out);
        }
    }

    private String convertToCsv(File lastraFile) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new LastraToCsvConverter(lastraFile).convert(out);
        return out.toString(StandardCharsets.UTF_8);
    }
}
