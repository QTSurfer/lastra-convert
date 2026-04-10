package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.parquet.Hydrator;
import com.wualabs.qtsurfer.parquet.HydratorSupplier;
import com.wualabs.qtsurfer.parquet.ParquetReader;
import com.wualabs.qtsurfer.lastra.Lastra;
import com.wualabs.qtsurfer.lastra.LastraWriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Converts a Parquet file to Lastra format using parquet-lite.
 *
 * <p>Usage:
 * <pre>{@code
 * var converter = ParquetToLastraConverter.builder(parquetFile)
 *     .map("timestamp", DataType.LONG, Codec.DELTA_VARINT)
 *     .map("open", DataType.DOUBLE, Codec.ALP)
 *     .map("high", DataType.DOUBLE, Codec.ALP)
 *     .map("low", DataType.DOUBLE, Codec.ALP)
 *     .map("close", DataType.DOUBLE, Codec.ALP)
 *     .map("volume", DataType.DOUBLE, Codec.ALP)
 *     .build();
 *
 * try (var out = new FileOutputStream("output.lastra")) {
 *     int rows = converter.convert(out);
 * }
 * }</pre>
 */
public final class ParquetToLastraConverter implements LastraConverter {

    private final File parquetFile;
    private final List<ColumnMapping> mappings;

    private ParquetToLastraConverter(File parquetFile, List<ColumnMapping> mappings) {
        this.parquetFile = parquetFile;
        this.mappings = List.copyOf(mappings);
    }

    @Override
    public int convert(OutputStream out) throws IOException {
        Collection<String> sourceColumns = mappings.stream()
                .map(ColumnMapping::sourceName)
                .collect(Collectors.toList());

        // Read all rows into column arrays
        List<Map<String, Object>> rows;
        try (Stream<Map<String, Object>> stream = ParquetReader.streamContent(parquetFile, new MapHydratorSupplier(), sourceColumns)) {
            rows = stream.collect(Collectors.toList());
        }

        if (rows.isEmpty()) {
            return 0;
        }

        int rowCount = rows.size();

        try (LastraWriter writer = new LastraWriter(out)) {
            // Register columns
            for (ColumnMapping m : mappings) {
                writer.addSeriesColumn(m.lastraName(), m.dataType(), m.codec(), m.metadata());
            }

            // Build column data arrays
            Object[] columnData = new Object[mappings.size()];
            for (int col = 0; col < mappings.size(); col++) {
                ColumnMapping m = mappings.get(col);
                columnData[col] = buildColumnArray(rows, m.sourceName(), m.dataType(), rowCount);
            }

            writer.writeSeries(rowCount, columnData);
            return rowCount;
        }
    }

    private Object buildColumnArray(List<Map<String, Object>> rows, String sourceName,
                                    Lastra.DataType dataType, int rowCount) {
        switch (dataType) {
            case LONG: {
                long[] arr = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    Object val = rows.get(i).get(sourceName);
                    arr[i] = val == null ? 0L : ((Number) val).longValue();
                }
                return arr;
            }
            case DOUBLE: {
                double[] arr = new double[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    Object val = rows.get(i).get(sourceName);
                    arr[i] = val == null ? 0.0 : ((Number) val).doubleValue();
                }
                return arr;
            }
            case BINARY: {
                byte[][] arr = new byte[rowCount][];
                for (int i = 0; i < rowCount; i++) {
                    Object val = rows.get(i).get(sourceName);
                    if (val instanceof byte[]) {
                        arr[i] = (byte[]) val;
                    } else if (val instanceof String) {
                        arr[i] = ((String) val).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    } else {
                        arr[i] = new byte[0];
                    }
                }
                return arr;
            }
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    public static Builder builder(File parquetFile) {
        return new Builder(parquetFile);
    }

    public static final class Builder {
        private final File parquetFile;
        private final List<ColumnMapping> mappings = new ArrayList<>();

        private Builder(File parquetFile) {
            this.parquetFile = parquetFile;
        }

        public Builder map(String name, Lastra.DataType dataType, Lastra.Codec codec) {
            mappings.add(ColumnMapping.of(name, dataType, codec));
            return this;
        }

        public Builder map(String sourceName, String lastraName, Lastra.DataType dataType, Lastra.Codec codec) {
            mappings.add(ColumnMapping.of(sourceName, lastraName, dataType, codec));
            return this;
        }

        public Builder map(ColumnMapping mapping) {
            mappings.add(mapping);
            return this;
        }

        public ParquetToLastraConverter build() {
            if (mappings.isEmpty()) {
                throw new IllegalStateException("At least one column mapping is required");
            }
            return new ParquetToLastraConverter(parquetFile, mappings);
        }
    }

    private static final class MapHydratorSupplier implements HydratorSupplier<Map<String, Object>, Map<String, Object>> {
        @Override
        public Hydrator<Map<String, Object>, Map<String, Object>> get(List<org.apache.parquet.column.ColumnDescriptor> columns) {
            return new Hydrator<Map<String, Object>, Map<String, Object>>() {
                @Override
                public Map<String, Object> start() {
                    return new HashMap<>();
                }

                @Override
                public Map<String, Object> add(Map<String, Object> target, String heading, Object value) {
                    target.put(heading, value);
                    return target;
                }

                @Override
                public Map<String, Object> finish(Map<String, Object> target) {
                    return target;
                }
            };
        }
    }
}
