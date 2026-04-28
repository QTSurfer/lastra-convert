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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
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
 *
 * <p>Row filtering — useful when the source parquet bundles many series in one file (e.g. an
 * hourly export carrying every {@code (mkt, ins)} pair) and the caller wants one Lastra blob per
 * subset:
 * <pre>{@code
 * var converter = ParquetToLastraConverter.builder(parquetFile)
 *     .map("t", "ts", DataType.LONG, Codec.DELTA_VARINT)
 *     .map("close", DataType.DOUBLE, Codec.ALP)
 *     .filter(row -> "binance".equals(row.get("mkt")) && "BTC/USDT".equals(row.get("ins")),
 *             "mkt", "ins")
 *     .build();
 * }</pre>
 * The {@code filterColumns} ({@code "mkt", "ins"} above) are read alongside the mapped columns but
 * are not written to the Lastra output.
 *
 * <p>Row transform — for time-bucket resampling, deduplication, or any row-list-level mutation
 * that needs the post-filter row set:
 * <pre>{@code
 * var converter = ParquetToLastraConverter.builder(parquetFile)
 *     .map("t", "ts", DataType.LONG, Codec.DELTA_VARINT)
 *     .map("close", DataType.DOUBLE, Codec.ALP)
 *     .filter(row -> "binance".equals(row.get("mkt")), "mkt")
 *     .transform(rows -> resample(rows, srcCadence, targetCadence))
 *     .build();
 * }</pre>
 * The transform runs after the filter and before column-array encoding; output rows still need
 * to expose the source-name columns the {@code map} declarations reference (unmapped extras are
 * ignored).
 */
public final class ParquetToLastraConverter implements LastraConverter {

    private final File parquetFile;
    private final List<ColumnMapping> mappings;
    private final Predicate<Map<String, Object>> rowFilter;
    private final List<String> filterColumns;
    private final UnaryOperator<List<Map<String, Object>>> rowTransform;

    private ParquetToLastraConverter(File parquetFile, List<ColumnMapping> mappings,
                                     Predicate<Map<String, Object>> rowFilter,
                                     List<String> filterColumns,
                                     UnaryOperator<List<Map<String, Object>>> rowTransform) {
        this.parquetFile = parquetFile;
        this.mappings = List.copyOf(mappings);
        this.rowFilter = rowFilter;
        this.filterColumns = List.copyOf(filterColumns);
        this.rowTransform = rowTransform;
    }

    @Override
    public int convert(OutputStream out) throws IOException {
        // Read source = mapped columns + any extra columns the row filter needs (deduped, mapped
        // columns first so the read order is stable for downstream consumers).
        LinkedHashSet<String> sourceColumns = new LinkedHashSet<>();
        for (ColumnMapping m : mappings) {
            sourceColumns.add(m.sourceName());
        }
        sourceColumns.addAll(filterColumns);

        List<Map<String, Object>> rows;
        try (Stream<Map<String, Object>> stream = ParquetReader.streamContent(parquetFile, new MapHydratorSupplier(), sourceColumns)) {
            Stream<Map<String, Object>> piped = rowFilter == null ? stream : stream.filter(rowFilter);
            rows = piped.collect(Collectors.toList());
        }

        // Optional row transform — applied AFTER filter, BEFORE column array build. The
        // transform may grow, shrink, or replace rows wholesale (e.g. resampling 1s bars to
        // 1m bars). Returning null is treated as "no rows", matching empty-input semantics.
        if (rowTransform != null) {
            List<Map<String, Object>> transformed = rowTransform.apply(rows);
            rows = transformed == null ? java.util.Collections.emptyList() : transformed;
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
        private Predicate<Map<String, Object>> rowFilter;
        private final List<String> filterColumns = new ArrayList<>();
        private UnaryOperator<List<Map<String, Object>>> rowTransform;

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

        /**
         * Drop rows that do not satisfy {@code predicate} before they reach the Lastra encoder.
         *
         * <p>The {@code Map} passed to the predicate carries the union of mapped {@code sourceName}s
         * and any {@code filterColumns} declared here. {@code filterColumns} are read from the
         * parquet alongside the mapped columns but are not written to the output Lastra; use them to
         * gate rows on metadata that is not part of the Lastra schema (e.g. a {@code mkt} or
         * {@code ins} column on a multi-instrument hourly export).
         *
         * <p>Calling this method twice replaces the previous filter.
         */
        public Builder filter(Predicate<Map<String, Object>> predicate, String... filterColumns) {
            this.rowFilter = predicate;
            this.filterColumns.clear();
            for (String c : filterColumns) {
                this.filterColumns.add(c);
            }
            return this;
        }

        /**
         * Apply a transform to the list of rows AFTER {@link #filter} but BEFORE column array
         * encoding. Use cases: time-bucket resampling (1s bars → 1m bars), down-/up-sampling,
         * row deduplication beyond what a stateless predicate can express. The transform may
         * grow or shrink the list; output rows must still carry the same source-name columns
         * the {@link #map} declarations reference (unmapped extras are ignored).
         *
         * <p>Returning {@code null} is treated as "no rows" — same as an empty source parquet.
         *
         * <p>Calling this method twice replaces the previous transform.
         */
        public Builder transform(UnaryOperator<List<Map<String, Object>>> transform) {
            this.rowTransform = transform;
            return this;
        }

        public ParquetToLastraConverter build() {
            if (mappings.isEmpty()) {
                throw new IllegalStateException("At least one column mapping is required");
            }
            return new ParquetToLastraConverter(parquetFile, mappings, rowFilter, filterColumns, rowTransform);
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
