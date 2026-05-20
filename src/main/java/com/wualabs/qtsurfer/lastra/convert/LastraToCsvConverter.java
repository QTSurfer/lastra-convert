package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.lastra.ColumnDescriptor;
import com.wualabs.qtsurfer.lastra.Lastra;
import com.wualabs.qtsurfer.lastra.LastraReader;
import com.wualabs.qtsurfer.lastra.RowGroup;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
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

        PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));

        // Header
        StringBuilder header = new StringBuilder();
        for (int c = 0; c < columns.size(); c++) {
            if (c > 0) header.append(delimiter);
            header.append(columns.get(c).name());
        }
        pw.println(header);

        // Stream row-group by row-group so heap residency stays bounded to one RG's
        // decoded primitive arrays — same reason as LastraToParquetConverter (#GOAL3).
        int totalWritten = 0;
        Iterator<RowGroup> rgIt = reader.readRowGroups();
        StringBuilder line = new StringBuilder();
        while (rgIt.hasNext()) {
            RowGroup rg = rgIt.next();
            int rgRows = rg.rowCount();
            Object[] columnData = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                switch (columns.get(i).dataType()) {
                    case LONG: columnData[i] = rg.getLongColumn(i); break;
                    case DOUBLE: columnData[i] = rg.getDoubleColumn(i); break;
                    case BINARY: columnData[i] = rg.getBinaryColumn(i); break;
                }
            }
            for (int row = 0; row < rgRows; row++) {
                line.setLength(0);
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
            totalWritten += rgRows;
        }

        pw.flush();
        if (totalWritten != rowCount) {
            throw new IOException(String.format(
                    "Row count mismatch: footer says %d but iterated %d", rowCount, totalWritten));
        }
        return totalWritten;
    }
}
