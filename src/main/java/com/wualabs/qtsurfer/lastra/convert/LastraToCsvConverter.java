package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.lastra.ColumnDescriptor;
import com.wualabs.qtsurfer.lastra.Lastra;
import com.wualabs.qtsurfer.lastra.LastraReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Converts a Lastra file to CSV format.
 *
 * <p>Writes series columns as comma-separated values with a header row. LONG values are written as
 * plain integers, DOUBLE with full precision, BINARY as UTF-8 strings (quoted if they contain
 * commas).
 */
public final class LastraToCsvConverter implements LastraConverter {

    private final File lastraFile;
    private final char delimiter;

    public LastraToCsvConverter(File lastraFile) {
        this(lastraFile, ',');
    }

    public LastraToCsvConverter(File lastraFile, char delimiter) {
        this.lastraFile = lastraFile;
        this.delimiter = delimiter;
    }

    @Override
    public int convert(OutputStream out) throws IOException {
        LastraReader reader = LastraReader.from(new FileInputStream(lastraFile));
        List<ColumnDescriptor> columns = reader.seriesColumns();
        int rowCount = reader.seriesRowCount();

        if (rowCount == 0 || columns.isEmpty()) {
            return 0;
        }

        // Read all column data
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

        // Write CSV
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));

        // Header
        StringBuilder header = new StringBuilder();
        for (int c = 0; c < columns.size(); c++) {
            if (c > 0) header.append(delimiter);
            header.append(columns.get(c).name());
        }
        pw.println(header);

        // Rows
        for (int row = 0; row < rowCount; row++) {
            StringBuilder line = new StringBuilder();
            for (int c = 0; c < columns.size(); c++) {
                if (c > 0) line.append(delimiter);
                ColumnDescriptor col = columns.get(c);
                switch (col.dataType()) {
                    case LONG:
                        line.append(((long[]) columnData[c])[row]);
                        break;
                    case DOUBLE:
                        line.append(BigDecimal.valueOf(((double[]) columnData[c])[row]).stripTrailingZeros().toPlainString());
                        break;
                    case BINARY:
                        String val = new String(((byte[][]) columnData[c])[row], StandardCharsets.UTF_8);
                        if (val.indexOf(delimiter) >= 0 || val.contains("\"")) {
                            line.append('"').append(val.replace("\"", "\"\"")).append('"');
                        } else {
                            line.append(val);
                        }
                        break;
                }
            }
            pw.println(line);
        }

        pw.flush();
        return rowCount;
    }
}
