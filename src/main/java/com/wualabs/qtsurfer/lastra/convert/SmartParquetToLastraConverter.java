package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.parquet.Hydrator;
import com.wualabs.qtsurfer.parquet.HydratorSupplier;
import com.wualabs.qtsurfer.parquet.ParquetReader;
import com.wualabs.qtsurfer.lastra.Lastra;
import com.wualabs.qtsurfer.lastra.Lastra.Codec;
import com.wualabs.qtsurfer.lastra.Lastra.DataType;
import com.wualabs.qtsurfer.lastra.LastraWriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Converts Parquet to Lastra with automatic codec selection per double column.
 *
 * <p>Reads all data first, then tries each codec per column to find the best one.
 */
public final class SmartParquetToLastraConverter implements LastraConverter {

    /** Use sample-based selection (fast) or full scan (optimal). */
    public enum Mode { SMART, BEST }

    private final File parquetFile;
    private final List<ColumnSpec> columns;
    private final Mode mode;

    private SmartParquetToLastraConverter(File parquetFile, List<ColumnSpec> columns, Mode mode) {
        this.parquetFile = parquetFile;
        this.columns = columns;
        this.mode = mode;
    }

    @Override
    public int convert(OutputStream out) throws IOException {
        List<String> sourceNames = columns.stream().map(c -> c.name).collect(Collectors.toList());

        // Read all rows
        List<Map<String, Object>> rows;
        try (Stream<Map<String, Object>> stream = ParquetReader.streamContent(parquetFile,
                new MapHydratorSupplier(), sourceNames)) {
            rows = stream.collect(Collectors.toList());
        }

        if (rows.isEmpty()) return 0;
        int rowCount = rows.size();

        // Build column arrays and select codecs
        Object[] columnData = new Object[columns.size()];
        Codec[] codecs = new Codec[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            ColumnSpec col = columns.get(i);
            columnData[i] = buildColumnArray(rows, col.name, col.dataType, rowCount);

            if (col.dataType == DataType.DOUBLE) {
                double[] data = (double[]) columnData[i];
                CodecSelector.Result result;
                if (mode == Mode.BEST) {
                    result = CodecSelector.selectByFullScan(data, rowCount);
                } else {
                    result = CodecSelector.selectBySample(data, rowCount);
                }
                codecs[i] = result.bestCodec();
                System.out.printf("  %s → %s [%s]%n", col.name, result.bestCodec(), result.report());
            } else {
                codecs[i] = col.codec;
                System.out.printf("  %s → %s%n", col.name, col.codec);
            }
        }

        // Write lastra
        try (LastraWriter writer = new LastraWriter(out)) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnSpec col = columns.get(i);
                writer.addSeriesColumn(col.name, col.dataType, codecs[i]);
            }
            writer.writeSeries(rowCount, columnData);
            return rowCount;
        }
    }

    private Object buildColumnArray(List<Map<String, Object>> rows, String name,
                                    DataType dataType, int rowCount) {
        switch (dataType) {
            case LONG: {
                long[] arr = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    Object val = rows.get(i).get(name);
                    arr[i] = val == null ? 0L : ((Number) val).longValue();
                }
                return arr;
            }
            case DOUBLE: {
                double[] arr = new double[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    Object val = rows.get(i).get(name);
                    arr[i] = val == null ? 0.0 : ((Number) val).doubleValue();
                }
                return arr;
            }
            case BINARY: {
                byte[][] arr = new byte[rowCount][];
                for (int i = 0; i < rowCount; i++) {
                    Object val = rows.get(i).get(name);
                    if (val instanceof byte[]) arr[i] = (byte[]) val;
                    else if (val instanceof String) arr[i] = ((String) val).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    else arr[i] = new byte[0];
                }
                return arr;
            }
            default:
                throw new IllegalArgumentException("Unsupported: " + dataType);
        }
    }

    public static SmartParquetToLastraConverter create(File parquetFile, Mode mode) throws IOException {
        var metadata = ParquetReader.readMetadata(parquetFile);
        var schema = metadata.getFileMetaData().getSchema();

        List<ColumnSpec> columns = new ArrayList<>();
        for (var col : schema.getColumns()) {
            String name = col.getPath()[0];
            var parquetType = col.getPrimitiveType().getPrimitiveTypeName();

            switch (parquetType) {
                case INT64: case INT32:
                    columns.add(new ColumnSpec(name, DataType.LONG, Codec.DELTA_VARINT));
                    break;
                case DOUBLE: case FLOAT:
                    columns.add(new ColumnSpec(name, DataType.DOUBLE, Codec.ALP)); // placeholder, will be overridden
                    break;
                case BINARY: case FIXED_LEN_BYTE_ARRAY:
                    columns.add(new ColumnSpec(name, DataType.BINARY, Codec.VARLEN_ZSTD));
                    break;
                case BOOLEAN:
                    columns.add(new ColumnSpec(name, DataType.LONG, Codec.RAW));
                    break;
                default:
                    System.err.println("Warning: skipping '" + name + "' (" + parquetType + ")");
            }
        }

        return new SmartParquetToLastraConverter(parquetFile, columns, mode);
    }

    static final class ColumnSpec {
        final String name;
        final DataType dataType;
        final Codec codec;

        ColumnSpec(String name, DataType dataType, Codec codec) {
            this.name = name;
            this.dataType = dataType;
            this.codec = codec;
        }
    }

    private static final class MapHydratorSupplier implements HydratorSupplier<Map<String, Object>, Map<String, Object>> {
        @Override
        public Hydrator<Map<String, Object>, Map<String, Object>> get(List<org.apache.parquet.column.ColumnDescriptor> columns) {
            return new Hydrator<Map<String, Object>, Map<String, Object>>() {
                @Override public Map<String, Object> start() { return new HashMap<>(); }
                @Override public Map<String, Object> add(Map<String, Object> t, String h, Object v) { t.put(h, v); return t; }
                @Override public Map<String, Object> finish(Map<String, Object> t) { return t; }
            };
        }
    }
}
