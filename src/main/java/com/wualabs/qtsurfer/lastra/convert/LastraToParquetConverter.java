package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.parquet.Dehydrator;
import com.wualabs.qtsurfer.parquet.ParquetWriter;
import com.wualabs.qtsurfer.parquet.ValueWriter;
import com.wualabs.qtsurfer.lastra.ColumnDescriptor;
import com.wualabs.qtsurfer.lastra.Lastra;
import com.wualabs.qtsurfer.lastra.LastraReader;
import com.wualabs.qtsurfer.lastra.RowGroup;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * Converts a Lastra file to Parquet format using parquet-lite.
 *
 * <p>Reads series columns from the Lastra file, maps them to a Parquet schema, and writes
 * ZSTD-compressed Parquet. Events section is not included (Parquet has no equivalent concept).
 *
 * <p>Usage:
 * <pre>{@code
 * var converter = new LastraToParquetConverter(lastraFile);
 * try (var out = new FileOutputStream("output.parquet")) {
 *     int rows = converter.convert(out);
 * }
 * }</pre>
 */
public final class LastraToParquetConverter implements LastraConverter {

    private final File lastraFile;

    public LastraToParquetConverter(File lastraFile) {
        this.lastraFile = lastraFile;
    }

    @Override
    public int convert(OutputStream out) throws IOException {
        LastraReader reader = LastraReader.from(new FileInputStream(lastraFile));
        List<ColumnDescriptor> columns = reader.seriesColumns();
        int rowCount = reader.seriesRowCount();

        if (rowCount == 0 || columns.isEmpty()) {
            return 0;
        }

        // Build Parquet schema from Lastra column descriptors
        MessageType schema = buildSchema(columns);

        // Stream row-group by row-group: pull each RG out of the Lastra reader, write
        // its rows to the Parquet writer, then advance — which clears the previous RG's
        // decoded arrays. Heap residency stays bounded to one Lastra row group's worth
        // of decoded primitive arrays plus the Parquet writer's internal page buffers,
        // independent of total dataset size. The previous implementation loaded every
        // column full-size before writing and OOM'd on 11.6M-row inputs under -Xmx512m.
        RowDehydrator dehydrator = new RowDehydrator(columns);
        int totalWritten = 0;
        try (ParquetWriter<Integer> writer = ParquetWriter.writeOutputStream(
                schema, out, dehydrator, CompressionCodecName.ZSTD)) {
            Iterator<RowGroup> rgIt = reader.readRowGroups();
            while (rgIt.hasNext()) {
                RowGroup rg = rgIt.next();
                int rgRows = rg.rowCount();
                // Materialise per-RG column arrays once (one decode per column per RG),
                // then drive the writer row-by-row over this RG only.
                Object[] columnData = new Object[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    switch (columns.get(i).dataType()) {
                        case LONG: columnData[i] = rg.getLongColumn(i); break;
                        case DOUBLE: columnData[i] = rg.getDoubleColumn(i); break;
                        case BINARY: columnData[i] = rg.getBinaryColumn(i); break;
                    }
                }
                dehydrator.setColumnData(columnData);
                for (int row = 0; row < rgRows; row++) {
                    writer.write(row);
                }
                totalWritten += rgRows;
                // columnData goes out of scope at loop bottom; rgIt.next() will then
                // call RowGroup.clear() on `rg` before yielding the next group, so the
                // decoded buffers become unreferenced.
            }
        }

        if (totalWritten != rowCount) {
            // Defensive check — would indicate a footer/data mismatch we should surface.
            throw new IOException(String.format(
                    "Row count mismatch: footer says %d but iterated %d", rowCount, totalWritten));
        }
        return totalWritten;
    }

    /**
     * Inspects a Lastra file and prints its structure.
     */
    public static void inspect(File lastraFile) throws IOException {
        LastraReader reader = LastraReader.from(new FileInputStream(lastraFile));
        System.out.println("Lastra file: " + lastraFile.getName());
        System.out.printf("  Series: %,d rows, %d columns%n",
                reader.seriesRowCount(), reader.seriesColumns().size());
        for (ColumnDescriptor col : reader.seriesColumns()) {
            System.out.printf("    %-12s %s / %s%n", col.name(), col.dataType(), col.codec());
            if (col.hasMetadata()) {
                System.out.printf("      metadata: %s%n", col.metadata());
            }
        }
        if (reader.eventsRowCount() > 0) {
            System.out.printf("  Events: %,d rows, %d columns%n",
                    reader.eventsRowCount(), reader.eventColumns().size());
            for (ColumnDescriptor col : reader.eventColumns()) {
                System.out.printf("    %-12s %s / %s%n", col.name(), col.dataType(), col.codec());
            }
        }
    }

    private static MessageType buildSchema(List<ColumnDescriptor> columns) {
        var fields = new org.apache.parquet.schema.Type[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescriptor col = columns.get(i);
            switch (col.dataType()) {
                case LONG:
                    // Detect timestamp columns by name
                    String name = col.name().toLowerCase();
                    if (name.equals("t") || name.equals("ts") || name.equals("timestamp")) {
                        fields[i] = Types.required(PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timestampType(
                                        true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                                .named(col.name());
                    } else {
                        fields[i] = Types.required(PrimitiveTypeName.INT64).named(col.name());
                    }
                    break;
                case DOUBLE:
                    fields[i] = Types.required(PrimitiveTypeName.DOUBLE).named(col.name());
                    break;
                case BINARY:
                    fields[i] = Types.required(PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.stringType())
                            .named(col.name());
                    break;
            }
        }
        return new MessageType("lastra", fields);
    }

    /**
     * Dehydrator that writes one row at a time from pre-read column arrays.
     * The row index is passed as the record.
     */
    private static final class RowDehydrator implements Dehydrator<Integer> {

        private final List<ColumnDescriptor> columns;
        private Object[] columnData;

        RowDehydrator(List<ColumnDescriptor> columns) {
            this.columns = columns;
        }

        void setColumnData(Object[] columnData) {
            this.columnData = columnData;
        }

        @Override
        public void dehydrate(Integer rowIndex, ValueWriter vw) {
            for (int col = 0; col < columns.size(); col++) {
                ColumnDescriptor desc = columns.get(col);
                switch (desc.dataType()) {
                    case LONG:
                        vw.write(desc.name(), ((long[]) columnData[col])[rowIndex]);
                        break;
                    case DOUBLE:
                        vw.write(desc.name(), ((double[]) columnData[col])[rowIndex]);
                        break;
                    case BINARY:
                        byte[] bytes = ((byte[][]) columnData[col])[rowIndex];
                        vw.write(desc.name(), new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
                        break;
                }
            }
        }
    }
}
