package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.lastra.Lastra;
import com.wualabs.qtsurfer.lastra.Lastra.Codec;
import com.wualabs.qtsurfer.lastra.Lastra.DataType;
import com.wualabs.qtsurfer.lastra.LastraWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a CSV file to Lastra format.
 *
 * <p>Auto-detects column types from the first data row:
 * <ul>
 *   <li>Integers and epoch timestamps → LONG / DELTA_VARINT
 *   <li>Decimal numbers → DOUBLE / ALP
 *   <li>Everything else → BINARY / VARLEN_ZSTD
 * </ul>
 *
 * <p>Supports comma, tab, and semicolon delimiters (auto-detected from header).
 */
public final class CsvToLastraConverter implements LastraConverter {

    private final File csvFile;
    private final List<ColumnMapping> explicitMappings;

    public CsvToLastraConverter(File csvFile) {
        this(csvFile, null);
    }

    public CsvToLastraConverter(File csvFile, List<ColumnMapping> mappings) {
        this.csvFile = csvFile;
        this.explicitMappings = mappings;
    }

    @Override
    public int convert(OutputStream out) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile, StandardCharsets.UTF_8))) {
            String headerLine = reader.readLine();
            if (headerLine == null) return 0;

            char delimiter = detectDelimiter(headerLine);
            String[] headers = split(headerLine, delimiter);

            // Read all rows
            List<String[]> rows = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                rows.add(split(line, delimiter));
            }

            if (rows.isEmpty()) return 0;

            // Determine column types
            List<ColumnMapping> mappings;
            if (explicitMappings != null && !explicitMappings.isEmpty()) {
                mappings = explicitMappings;
            } else {
                mappings = autoDetect(headers, rows.get(0));
                for (ColumnMapping m : mappings) {
                    System.out.printf("  %s → %s / %s%n", m.lastraName(), m.dataType(), m.codec());
                }
            }

            // Build column arrays
            int rowCount = rows.size();
            Object[] columnData = new Object[mappings.size()];

            for (int col = 0; col < mappings.size(); col++) {
                ColumnMapping m = mappings.get(col);
                int srcIdx = findHeader(headers, m.sourceName());
                if (srcIdx < 0) {
                    throw new IOException("Column not found in CSV: " + m.sourceName());
                }

                switch (m.dataType()) {
                    case LONG: {
                        long[] arr = new long[rowCount];
                        for (int r = 0; r < rowCount; r++) {
                            String[] row = rows.get(r);
                            arr[r] = srcIdx < row.length ? parseLong(row[srcIdx]) : 0L;
                        }
                        columnData[col] = arr;
                        break;
                    }
                    case DOUBLE: {
                        double[] arr = new double[rowCount];
                        for (int r = 0; r < rowCount; r++) {
                            String[] row = rows.get(r);
                            arr[r] = srcIdx < row.length ? parseDouble(row[srcIdx]) : 0.0;
                        }
                        columnData[col] = arr;
                        break;
                    }
                    case BINARY: {
                        byte[][] arr = new byte[rowCount][];
                        for (int r = 0; r < rowCount; r++) {
                            String[] row = rows.get(r);
                            String val = srcIdx < row.length ? row[srcIdx] : "";
                            arr[r] = val.getBytes(StandardCharsets.UTF_8);
                        }
                        columnData[col] = arr;
                        break;
                    }
                }
            }

            // Write Lastra
            try (LastraWriter writer = new LastraWriter(out)) {
                for (ColumnMapping m : mappings) {
                    writer.addSeriesColumn(m.lastraName(), m.dataType(), m.codec());
                }
                writer.writeSeries(rowCount, columnData);
                return rowCount;
            }
        }
    }

    static List<ColumnMapping> autoDetect(String[] headers, String[] firstRow) {
        List<ColumnMapping> mappings = new ArrayList<>();
        for (int i = 0; i < headers.length; i++) {
            String name = headers[i].trim();
            String sample = i < firstRow.length ? firstRow[i].trim() : "";

            DataType type;
            Codec codec;

            if (isLong(sample)) {
                type = DataType.LONG;
                codec = Codec.DELTA_VARINT;
            } else if (isDouble(sample)) {
                type = DataType.DOUBLE;
                codec = Codec.ALP;
            } else {
                type = DataType.BINARY;
                codec = Codec.VARLEN_ZSTD;
            }

            mappings.add(ColumnMapping.of(name, type, codec));
        }
        return mappings;
    }

    private static boolean isLong(String s) {
        try {
            Long.parseLong(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isDouble(String s) {
        try {
            Double.parseDouble(s);
            return s.contains(".") || s.contains("e") || s.contains("E");
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static long parseLong(String s) {
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static double parseDouble(String s) {
        try {
            return Double.parseDouble(s.trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static int findHeader(String[] headers, String name) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equals(name)) return i;
        }
        return -1;
    }

    static char detectDelimiter(String header) {
        int commas = count(header, ',');
        int tabs = count(header, '\t');
        int semis = count(header, ';');
        if (tabs >= commas && tabs >= semis) return '\t';
        if (semis > commas) return ';';
        return ',';
    }

    private static int count(String s, char c) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == c) n++;
        }
        return n;
    }

    private static String[] split(String line, char delimiter) {
        return line.split(String.valueOf(delimiter), -1);
    }
}
