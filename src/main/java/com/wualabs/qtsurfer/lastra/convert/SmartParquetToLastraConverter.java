package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.parquet.Hydrator;
import com.wualabs.qtsurfer.parquet.HydratorSupplier;
import com.wualabs.qtsurfer.parquet.ParquetReader;
import com.wualabs.qtsurfer.lastra.Lastra.Codec;
import com.wualabs.qtsurfer.lastra.Lastra.DataType;
import com.wualabs.qtsurfer.lastra.LastraWriter;
import org.apache.parquet.column.ColumnDescriptor;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts Parquet to Lastra with automatic codec selection per double column.
 *
 * <p>Streams the parquet input row-group by row-group: fills a fixed-size batch of
 * primitive column arrays, flushes the batch to {@link LastraWriter} (which produces
 * one Lastra row-group per flush), then reuses the same arrays for the next batch.
 * Heap residency is O(batchSize × columns), not O(dataset), so a 286 MB / 11.6M-row
 * parquet converts under -Xmx512m instead of the >4 GB the previous map-hydrating
 * implementation required (#93b / lastra-convert 0.14.0).
 *
 * <p>Codec selection (when {@link Mode#SMART} or {@link Mode#BEST}) runs once on the
 * first full batch and is locked-in for the rest of the stream. The previous
 * "full scan" mode that required holding the entire dataset in heap was removed in
 * 0.14.0 (clean cut, no compat flag); if honest-best vs full dataset is needed, the
 * caller must pre-partition the input.
 */
public final class SmartParquetToLastraConverter implements LastraConverter {

    /** Use sample-based selection ({@link CodecSelector#selectBySample}) — the only mode in 0.14+. */
    public enum Mode { SMART, BEST }

    /** Batch size in rows. Matches {@link LastraWriter#DEFAULT_ROW_GROUP_SIZE}. */
    public static final int DEFAULT_BATCH_ROWS = 4_096;

    private final File parquetFile;
    private final List<ColumnSpec> columns;
    private final Mode mode;
    private final int batchRows;

    private SmartParquetToLastraConverter(File parquetFile, List<ColumnSpec> columns,
                                          Mode mode, int batchRows) {
        this.parquetFile = parquetFile;
        this.columns = columns;
        this.mode = mode;
        this.batchRows = batchRows;
    }

    @Override
    public int convert(OutputStream out) throws IOException {
        List<String> sourceNames = columns.stream().map(c -> c.name).collect(Collectors.toList());

        // Pre-allocate column arrays sized to one batch. Reused across all batches —
        // each flush overwrites from index 0 again.
        Object[] columnArrays = allocateBatchArrays(batchRows);
        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) nameToIndex.put(columns.get(i).name, i);

        // Cursor + reusable hydrator that writes into the pre-allocated arrays.
        BatchCursor cursor = new BatchCursor();
        BatchHydratorSupplier supplier = new BatchHydratorSupplier(columns, columnArrays, nameToIndex, cursor);

        // Codecs are determined from the first full batch and locked in for the rest.
        Codec[] codecs = null;
        int totalRows = 0;

        try (LastraWriter writer = new LastraWriter(out);
             ParquetReader<int[], int[]> reader = ParquetReader.spliterator(parquetFile, supplier, sourceNames)) {

            // Drive the spliterator manually so we know when to flush.
            while (reader.tryAdvance(unused -> { /* hydrator already wrote into columnArrays at cursor.idx */ })) {
                if (cursor.idx == batchRows) {
                    if (codecs == null) {
                        codecs = selectCodecsAndDeclare(writer, columnArrays, batchRows);
                    }
                    writer.writeSeries(batchRows, columnArrays);
                    totalRows += batchRows;
                    cursor.idx = 0;
                }
            }

            // Flush the (possibly partial) final batch.
            if (cursor.idx > 0) {
                if (codecs == null) {
                    codecs = selectCodecsAndDeclare(writer, columnArrays, cursor.idx);
                }
                writer.writeSeries(cursor.idx, columnArrays);
                totalRows += cursor.idx;
                cursor.idx = 0;
            } else if (codecs == null) {
                // Empty input — still need to declare columns so the lastra file has a valid header.
                declareColumnsWithDefaults(writer);
            }
        }

        return totalRows;
    }

    /**
     * Sample the first batch for codec selection (only DOUBLE columns; LONG/BINARY use their
     * declared defaults). Declares all columns on the writer in column order, returns the
     * locked-in codec array.
     */
    private Codec[] selectCodecsAndDeclare(LastraWriter writer, Object[] columnArrays, int sampleRows) {
        Codec[] codecs = new Codec[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            ColumnSpec col = columns.get(i);
            if (col.dataType == DataType.DOUBLE) {
                double[] sample = (double[]) columnArrays[i];
                // selectBySample picks from a small subset; selectByFullScan exhaustively tests
                // codecs against the sample we already have. Both bounded by batchRows.
                CodecSelector.Result result = (mode == Mode.BEST)
                        ? CodecSelector.selectByFullScan(sample, sampleRows)
                        : CodecSelector.selectBySample(sample, sampleRows);
                codecs[i] = result.bestCodec();
                System.out.printf("  %s → %s [%s]%n", col.name, result.bestCodec(), result.report());
            } else {
                codecs[i] = col.codec;
                System.out.printf("  %s → %s%n", col.name, col.codec);
            }
            writer.addSeriesColumn(col.name, col.dataType, codecs[i]);
        }
        return codecs;
    }

    private void declareColumnsWithDefaults(LastraWriter writer) {
        for (ColumnSpec col : columns) {
            writer.addSeriesColumn(col.name, col.dataType, col.codec);
        }
    }

    private Object[] allocateBatchArrays(int capacity) {
        Object[] arr = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            switch (columns.get(i).dataType) {
                case LONG:
                    arr[i] = new long[capacity];
                    break;
                case DOUBLE:
                    arr[i] = new double[capacity];
                    break;
                case BINARY:
                    arr[i] = new byte[capacity][];
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported: " + columns.get(i).dataType);
            }
        }
        return arr;
    }

    public static SmartParquetToLastraConverter create(File parquetFile, Mode mode) throws IOException {
        return create(parquetFile, mode, DEFAULT_BATCH_ROWS);
    }

    public static SmartParquetToLastraConverter create(File parquetFile, Mode mode, int batchRows)
            throws IOException {
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
                    columns.add(new ColumnSpec(name, DataType.DOUBLE, Codec.ALP)); // placeholder, overridden by selector
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

        return new SmartParquetToLastraConverter(parquetFile, columns, mode, batchRows);
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

    /** Mutable batch position. Shared between converter loop and the hydrator. */
    private static final class BatchCursor {
        int idx;
    }

    /**
     * Hydrator that writes directly into pre-allocated primitive arrays at the current
     * cursor position. {@code start()} returns a shared sentinel (no per-row alloc).
     * {@code add()} routes value to the correct column array slot. {@code finish()}
     * advances the cursor.
     */
    private static final class BatchHydratorSupplier
            implements HydratorSupplier<int[], int[]> {
        private static final int[] SENTINEL = new int[0];

        private final List<ColumnSpec> columns;
        private final Object[] columnArrays;
        private final Map<String, Integer> nameToIndex;
        private final BatchCursor cursor;

        BatchHydratorSupplier(List<ColumnSpec> columns, Object[] columnArrays,
                              Map<String, Integer> nameToIndex, BatchCursor cursor) {
            this.columns = columns;
            this.columnArrays = columnArrays;
            this.nameToIndex = nameToIndex;
            this.cursor = cursor;
        }

        @Override
        public Hydrator<int[], int[]> get(List<ColumnDescriptor> parquetCols) {
            return new Hydrator<int[], int[]>() {
                @Override public int[] start() { return SENTINEL; }

                @Override public int[] add(int[] target, String heading, Object value) {
                    Integer ci = nameToIndex.get(heading);
                    if (ci == null) return target; // column not in our schema, skip
                    int slot = cursor.idx;
                    switch (columns.get(ci).dataType) {
                        case LONG:
                            ((long[]) columnArrays[ci])[slot] = (value == null) ? 0L : ((Number) value).longValue();
                            break;
                        case DOUBLE:
                            ((double[]) columnArrays[ci])[slot] = (value == null) ? 0.0 : ((Number) value).doubleValue();
                            break;
                        case BINARY:
                            byte[][] arr = (byte[][]) columnArrays[ci];
                            if (value == null) {
                                arr[slot] = null;
                            } else if (value instanceof byte[]) {
                                arr[slot] = (byte[]) value;
                            } else if (value instanceof String) {
                                arr[slot] = ((String) value).getBytes(StandardCharsets.UTF_8);
                            } else {
                                arr[slot] = value.toString().getBytes(StandardCharsets.UTF_8);
                            }
                            break;
                    }
                    return target;
                }

                @Override public int[] finish(int[] target) {
                    cursor.idx++;
                    return target;
                }
            };
        }
    }
}
