package com.wualabs.qtsurfer.reef.convert;

import com.wualabs.qtsurfer.parquet.Dehydrator;
import com.wualabs.qtsurfer.parquet.ParquetWriter;
import com.wualabs.qtsurfer.parquet.ValueWriter;
import com.wualabs.qtsurfer.reef.ColumnDescriptor;
import com.wualabs.qtsurfer.reef.Reef;
import com.wualabs.qtsurfer.reef.ReefReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * Converts a Reef file to Parquet format using parquet-lite.
 *
 * <p>Reads series columns from the Reef file, maps them to a Parquet schema, and writes
 * ZSTD-compressed Parquet. Events section is not included (Parquet has no equivalent concept).
 *
 * <p>Usage:
 * <pre>{@code
 * var converter = new ReefToParquetConverter(reefFile);
 * try (var out = new FileOutputStream("output.parquet")) {
 *     int rows = converter.convert(out);
 * }
 * }</pre>
 */
public final class ReefToParquetConverter implements ReefConverter {

    private final File reefFile;

    public ReefToParquetConverter(File reefFile) {
        this.reefFile = reefFile;
    }

    @Override
    public int convert(OutputStream out) throws IOException {
        ReefReader reader = ReefReader.from(new FileInputStream(reefFile));
        List<ColumnDescriptor> columns = reader.seriesColumns();
        int rowCount = reader.seriesRowCount();

        if (rowCount == 0 || columns.isEmpty()) {
            return 0;
        }

        // Build Parquet schema from Reef column descriptors
        MessageType schema = buildSchema(columns);

        // Read all column data from Reef
        Object[] columnData = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescriptor col = columns.get(i);
            switch (col.dataType()) {
                case LONG:
                    columnData[i] = reader.readSeriesLong(col.name());
                    break;
                case DOUBLE:
                    columnData[i] = reader.readSeriesDouble(col.name());
                    break;
                case BINARY:
                    columnData[i] = reader.readSeriesBinary(col.name());
                    break;
            }
        }

        // Write Parquet
        RowDehydrator dehydrator = new RowDehydrator(columns);
        try (ParquetWriter<Integer> writer = ParquetWriter.writeOutputStream(
                schema, out, dehydrator, CompressionCodecName.ZSTD)) {
            dehydrator.setColumnData(columnData);
            for (int row = 0; row < rowCount; row++) {
                writer.write(row);
            }
        }

        return rowCount;
    }

    /**
     * Inspects a Reef file and prints its structure.
     */
    public static void inspect(File reefFile) throws IOException {
        ReefReader reader = ReefReader.from(new FileInputStream(reefFile));
        System.out.println("Reef file: " + reefFile.getName());
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
        return new MessageType("reef", fields);
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
