package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.lastra.LastraReader;
import com.wualabs.qtsurfer.parquet.Hydrator;
import com.wualabs.qtsurfer.parquet.HydratorSupplier;
import com.wualabs.qtsurfer.parquet.ParquetReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Row-by-row comparator between cache-v2 Lastra segments and QDB Parquet hourly backups.
 *
 * <p>Usage:
 * <pre>
 *   java -cp lastra-convert.jar com.wualabs.qtsurfer.lastra.convert.LastraParquetCompare \
 *       &lt;instrument&gt; &lt;lastra-hour-dir&gt; &lt;parquet-file&gt;
 * </pre>
 *
 * <p>Expected layouts:
 * <ul>
 *   <li>Lastra dir: {@code /data/v2/{exchange}/{INSTRUMENT}/{YYYY-MM-DD}/{HH}/*.lastra} — many
 *       small segments, each containing columns {@code opn,hig,low,cls,vol,vlq,bid,bsz,ask,asz,ts}
 *       with ts in <b>milliseconds</b>.</li>
 *   <li>Parquet: QDB export of {@code tickers} table filtered to {@code WHERE t IN 'YYYY-MM-DDTHH'}.
 *       Columns: {@code mkt,ins,opn,hig,low,cls,vol,vlq,bid,bsz,ask,asz,t} with t in
 *       <b>microseconds</b>.</li>
 * </ul>
 *
 * <p>Timestamps are normalized to microseconds for comparison. Exits with non-zero status if any
 * mismatch is found; prints a per-category summary regardless.
 */
public final class LastraParquetCompare {

    private static final String[] VALUE_COLS = {
            "opn", "hig", "low", "cls", "vol", "vlq", "bid", "bsz", "ask", "asz"
    };

    private static final double EPSILON = 1e-9;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: LastraParquetCompare <instrument> <lastra-day-dir> <parquet-file> <hour>");
            System.err.println("  instrument:       BTC/USDT (slash form, matches ins column)");
            System.err.println("  lastra-day-dir:   /data/v2/binance/BTC_USDT/2026-04-17");
            System.err.println("  parquet-file:     /backup/2026-04/17/tickers_2026-04-17_h07.parquet");
            System.err.println("  hour:             07 (0-23, optional — if set, also reads hour-1 and hour+1");
            System.err.println("                     from lastra to capture batch spillover, and filters");
            System.err.println("                     lastra rows to ts in [hour:00:00, hour:59:59.999])");
            System.exit(2);
        }
        String instrument = args[0];
        Path dayOrHourDir = Path.of(args[1]);
        File parquetFile = new File(args[2]);
        int hour = args.length >= 4 ? Integer.parseInt(args[3]) : -1;

        System.out.println("[compare] instrument=" + instrument);
        System.out.println("[compare] lastra-root=" + dayOrHourDir + (hour >= 0 ? " hour=" + hour : ""));
        System.out.println("[compare] parquet=" + parquetFile);

        long t0 = System.currentTimeMillis();
        List<Row> lastraRows;
        if (Files.isRegularFile(dayOrHourDir)) {
            // Single hourly Lastra file (new layout: {INST}/{day}/h{HH}.lastra)
            lastraRows = loadLastraFile(dayOrHourDir);
        } else if (hour >= 0) {
            lastraRows = loadLastraWithSpillover(dayOrHourDir, hour);
        } else {
            lastraRows = loadLastraHour(dayOrHourDir);
        }
        long t1 = System.currentTimeMillis();
        System.out.printf("[compare] lastra: %,d rows in %d ms%n", lastraRows.size(), t1 - t0);

        List<Row> parquetRows = loadParquetForInstrument(parquetFile, instrument);
        long t2 = System.currentTimeMillis();
        System.out.printf("[compare] parquet: %,d rows in %d ms%n", parquetRows.size(), t2 - t1);

        lastraRows.sort((a, b) -> Long.compare(a.tsUs, b.tsUs));
        parquetRows.sort((a, b) -> Long.compare(a.tsUs, b.tsUs));

        int i = 0, j = 0;
        int matched = 0, diffs = 0, missingInLastra = 0, missingInParquet = 0;
        int maxDiffsShown = 10, diffsShown = 0;

        while (i < lastraRows.size() && j < parquetRows.size()) {
            Row l = lastraRows.get(i);
            Row p = parquetRows.get(j);
            if (l.tsUs == p.tsUs) {
                String mismatch = compareValues(l, p);
                if (mismatch == null) {
                    matched++;
                } else {
                    diffs++;
                    if (diffsShown < maxDiffsShown) {
                        System.out.printf("[diff] ts=%d  %s%n", l.tsUs, mismatch);
                        diffsShown++;
                    }
                }
                i++;
                j++;
            } else if (l.tsUs < p.tsUs) {
                missingInParquet++;
                if (missingInParquet <= maxDiffsShown) {
                    System.out.printf("[only-in-lastra] ts=%d %s%n", l.tsUs, rowBrief(l));
                }
                i++;
            } else {
                missingInLastra++;
                if (missingInLastra <= maxDiffsShown) {
                    System.out.printf("[only-in-parquet] ts=%d %s%n", p.tsUs, rowBrief(p));
                }
                j++;
            }
        }
        while (i < lastraRows.size()) {
            missingInParquet++;
            if (missingInParquet <= maxDiffsShown) {
                System.out.printf("[only-in-lastra] ts=%d %s%n",
                        lastraRows.get(i).tsUs, rowBrief(lastraRows.get(i)));
            }
            i++;
        }
        while (j < parquetRows.size()) {
            missingInLastra++;
            if (missingInLastra <= maxDiffsShown) {
                System.out.printf("[only-in-parquet] ts=%d %s%n",
                        parquetRows.get(j).tsUs, rowBrief(parquetRows.get(j)));
            }
            j++;
        }

        long t3 = System.currentTimeMillis();
        System.out.println();
        System.out.println("============ summary ============");
        System.out.printf("  matched (ts + all values):       %,d%n", matched);
        System.out.printf("  value mismatches at same ts:     %,d%n", diffs);
        System.out.printf("  only in lastra (missing in pqt): %,d%n", missingInParquet);
        System.out.printf("  only in parquet (missing in l):  %,d%n", missingInLastra);
        System.out.printf("  total compared in %d ms%n", t3 - t0);
        System.out.println("=================================");

        boolean anyDiff = (diffs + missingInLastra + missingInParquet) > 0;
        System.exit(anyDiff ? 1 : 0);
    }

    /**
     * Load rows whose timestamp falls inside [hour:00, (hour+1):00) by scanning hour-1, hour and
     * hour+1 subdirectories under {@code dayDir}. This compensates for cache-v2's practice of
     * placing a batch's segment under the hour of its FIRST message — which means early messages
     * of an hour end up in the previous hour's folder when they land in a batch opened before the
     * boundary, and late messages straddle into the next hour's folder when a batch carries over.
     */
    private static List<Row> loadLastraWithSpillover(Path dayDir, int hour) throws IOException {
        long hourStartUs = hourToEpochMicros(dayDir.getFileName().toString(), hour);
        long hourEndUs = hourStartUs + 3_600_000_000L;

        List<Row> all = new ArrayList<>();
        for (int h = hour - 1; h <= hour + 1; h++) {
            if (h < 0 || h > 23) continue;
            Path dir = dayDir.resolve(String.format("%02d", h));
            if (!Files.isDirectory(dir)) continue;
            List<Row> part = loadLastraHour(dir);
            System.out.printf("[compare] read %d lastra rows from %s%n", part.size(), dir.getFileName());
            all.addAll(part);
        }
        List<Row> filtered = new ArrayList<>(all.size());
        for (Row r : all) {
            if (r.tsUs >= hourStartUs && r.tsUs < hourEndUs) filtered.add(r);
        }
        System.out.printf("[compare] kept %d rows in [%d, %d)%n", filtered.size(), hourStartUs, hourEndUs);
        return filtered;
    }

    private static long hourToEpochMicros(String yyyymmdd, int hour) {
        // yyyymmdd in "YYYY-MM-DD"
        java.time.LocalDate d = java.time.LocalDate.parse(yyyymmdd);
        java.time.Instant ins = d.atTime(hour, 0).toInstant(java.time.ZoneOffset.UTC);
        return ins.getEpochSecond() * 1_000_000L;
    }

    private static List<Row> loadLastraFile(Path file) throws IOException {
        List<Row> out = new ArrayList<>();
        try (FileInputStream in = new FileInputStream(file.toFile())) {
            LastraReader reader = LastraReader.from(in);
            int n = reader.seriesRowCount();
            if (n == 0) return out;
            long[] ts = reader.readSeriesLong("ts");
            double[] opn = reader.readSeriesDouble("opn");
            double[] hig = reader.readSeriesDouble("hig");
            double[] low = reader.readSeriesDouble("low");
            double[] cls = reader.readSeriesDouble("cls");
            double[] vol = reader.readSeriesDouble("vol");
            double[] vlq = reader.readSeriesDouble("vlq");
            double[] bid = reader.readSeriesDouble("bid");
            double[] bsz = reader.readSeriesDouble("bsz");
            double[] ask = reader.readSeriesDouble("ask");
            double[] asz = reader.readSeriesDouble("asz");
            for (int r = 0; r < n; r++) {
                Row row = new Row();
                row.tsUs = ts[r] * 1000L;
                row.values[0] = opn[r]; row.values[1] = hig[r]; row.values[2] = low[r];
                row.values[3] = cls[r]; row.values[4] = vol[r]; row.values[5] = vlq[r];
                row.values[6] = bid[r]; row.values[7] = bsz[r]; row.values[8] = ask[r];
                row.values[9] = asz[r];
                out.add(row);
            }
        }
        return out;
    }

    private static List<Row> loadLastraHour(Path hourDir) throws IOException {
        if (!Files.isDirectory(hourDir)) {
            throw new IOException("not a directory: " + hourDir);
        }
        List<Path> files;
        try (Stream<Path> s = Files.list(hourDir)) {
            files = s.filter(p -> p.getFileName().toString().endsWith(".lastra"))
                    .sorted()
                    .collect(Collectors.toList());
        }
        List<Row> out = new ArrayList<>();
        for (Path f : files) {
            try (FileInputStream in = new FileInputStream(f.toFile())) {
                LastraReader reader = LastraReader.from(in);
                List<com.wualabs.qtsurfer.lastra.ColumnDescriptor> cols = reader.seriesColumns();
                int n = reader.seriesRowCount();
                if (n == 0) continue;

                Map<String, Object> data = new HashMap<>();
                for (com.wualabs.qtsurfer.lastra.ColumnDescriptor c : cols) {
                    switch (c.dataType()) {
                        case LONG:
                            data.put(c.name(), reader.readSeriesLong(c.name()));
                            break;
                        case DOUBLE:
                            data.put(c.name(), reader.readSeriesDouble(c.name()));
                            break;
                        default:
                            break;
                    }
                }

                long[] ts = (long[]) data.get("ts");
                double[] opn = (double[]) data.get("opn");
                double[] hig = (double[]) data.get("hig");
                double[] low = (double[]) data.get("low");
                double[] cls = (double[]) data.get("cls");
                double[] vol = (double[]) data.get("vol");
                double[] vlq = (double[]) data.get("vlq");
                double[] bid = (double[]) data.get("bid");
                double[] bsz = (double[]) data.get("bsz");
                double[] ask = (double[]) data.get("ask");
                double[] asz = (double[]) data.get("asz");

                for (int r = 0; r < n; r++) {
                    Row row = new Row();
                    row.tsUs = ts[r] * 1000L; // ms → μs
                    row.values[0] = opn[r];
                    row.values[1] = hig[r];
                    row.values[2] = low[r];
                    row.values[3] = cls[r];
                    row.values[4] = vol[r];
                    row.values[5] = vlq[r];
                    row.values[6] = bid[r];
                    row.values[7] = bsz[r];
                    row.values[8] = ask[r];
                    row.values[9] = asz[r];
                    out.add(row);
                }
            }
        }
        return out;
    }

    private static List<Row> loadParquetForInstrument(File parquetFile, String instrument) throws IOException {
        List<String> needed = new ArrayList<>();
        needed.add("ins");
        needed.add("t");
        needed.addAll(Arrays.asList(VALUE_COLS));

        String normalizedWanted = instrument; // e.g. BTC/USDT
        List<Row> out = new ArrayList<>();
        try (Stream<Map<String, Object>> stream = ParquetReader.streamContent(
                parquetFile, new MapHydratorSupplier(), needed)) {
            stream.forEach(m -> {
                Object ins = m.get("ins");
                String insStr = ins == null ? null : ins.toString();
                if (insStr == null || !insStr.equals(normalizedWanted)) return;
                Row row = new Row();
                // QDB TIMESTAMP in parquet is INT64 microseconds since epoch.
                Object t = m.get("t");
                row.tsUs = ((Number) t).longValue();
                for (int k = 0; k < VALUE_COLS.length; k++) {
                    Object v = m.get(VALUE_COLS[k]);
                    row.values[k] = v == null ? 0.0 : ((Number) v).doubleValue();
                }
                out.add(row);
            });
        }
        return out;
    }

    private static String compareValues(Row a, Row b) {
        StringBuilder sb = null;
        for (int k = 0; k < VALUE_COLS.length; k++) {
            if (!almostEqual(a.values[k], b.values[k])) {
                if (sb == null) sb = new StringBuilder();
                else sb.append(", ");
                sb.append(String.format(Locale.ROOT, "%s: lastra=%s parquet=%s",
                        VALUE_COLS[k], a.values[k], b.values[k]));
            }
        }
        return sb == null ? null : sb.toString();
    }

    private static boolean almostEqual(double a, double b) {
        if (a == b) return true;
        double diff = Math.abs(a - b);
        double scale = Math.max(Math.abs(a), Math.abs(b));
        return diff <= EPSILON * Math.max(1.0, scale);
    }

    private static String rowBrief(Row r) {
        return String.format(Locale.ROOT, "opn=%.8g cls=%.8g bid=%.8g ask=%.8g",
                r.values[0], r.values[3], r.values[6], r.values[8]);
    }

    private static final class Row {
        long tsUs;
        final double[] values = new double[VALUE_COLS.length];
    }

    private static final class MapHydratorSupplier
            implements HydratorSupplier<Map<String, Object>, Map<String, Object>> {
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
