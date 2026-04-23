package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.lastra.Lastra;
import com.wualabs.qtsurfer.lastra.LastraWriter;
import com.wualabs.qtsurfer.parquet.Hydrator;
import com.wualabs.qtsurfer.parquet.HydratorSupplier;
import com.wualabs.qtsurfer.parquet.ParquetReader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Backfills hourly Lastra files per instrument from a QDB {@code klines} Parquet hourly dump.
 * Produces one {@code {base}/{exchange}/{INSTRUMENT}/{YYYY-MM}/{DD}/klines/h{HH}.lastra} per
 * distinct {@code ins} value found in the parquet.
 *
 * <p>Companion to {@link LastraHourlyBackfill} (which handles tickers). The schemas differ: klines
 * carries OHLCV + trade count (integer) and lacks the bid/ask sizes, so we can't reuse the tickers
 * path directly. Output layout and row-grouping convention match.
 *
 * <p>Schema (matches QDB {@code klines}): series columns
 * {@code ts,opn,hig,low,cls,vol,vlq,ntr}; ts stored in <b>milliseconds</b> (µs / 1000). {@code ntr}
 * is a LONG (trade count). Rows are sorted by ts before writing.
 *
 * <p>Usage:
 * <pre>
 *   java -cp lastra-convert.jar com.wualabs.qtsurfer.lastra.convert.LastraKlinesHourlyBackfill \
 *       &lt;exchange&gt; &lt;parquet-file-or-dir&gt; &lt;base-out-dir&gt; [--skip-existing] [--from YYYY-MM-DD] [--to YYYY-MM-DD]
 * </pre>
 * Parquet filename must end in {@code _YYYY-MM-DD_hHH.parquet} and start with {@code klines_}
 * (discovery walks for {@code klines_*.parquet}).
 */
public final class LastraKlinesHourlyBackfill {

    private static final String[] DOUBLE_COLS = {
            "opn", "hig", "low", "cls", "vol", "vlq"
    };

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: LastraKlinesHourlyBackfill <exchange> <parquet-file-or-dir> <base-out-dir> [--skip-existing] [--from YYYY-MM-DD] [--to YYYY-MM-DD]");
            System.err.println("  exchange:            e.g. binance");
            System.err.println("  parquet-file-or-dir: single parquet file, or directory walked recursively for klines_*.parquet");
            System.err.println("  base-out-dir:        e.g. /data/v2 (writes under {base}/{exchange}/{INST}/{day}/h{HH}.lastra)");
            System.err.println("  --skip-existing:     skip parquets whose output already exists for every instrument");
            System.err.println("  --from / --to:       inclusive date filter on the parquet filename's day token");
            System.exit(2);
        }
        String exchange = args[0];
        Path root = Path.of(args[1]);
        Path baseDir = Path.of(args[2]);
        boolean skipExisting = false;
        java.time.LocalDate from = null, to = null;
        for (int k = 3; k < args.length; k++) {
            switch (args[k]) {
                case "--skip-existing":
                    skipExisting = true;
                    break;
                case "--from":
                    from = java.time.LocalDate.parse(args[++k]);
                    break;
                case "--to":
                    to = java.time.LocalDate.parse(args[++k]);
                    break;
                default:
                    System.err.println("unknown arg: " + args[k]);
                    System.exit(2);
            }
        }

        List<Path> parquets = discoverParquets(root, from, to);
        System.out.printf("[klines-backfill] exchange=%s base=%s parquets=%d skipExisting=%s%n",
                exchange, baseDir, parquets.size(), skipExisting);
        if (parquets.isEmpty()) {
            System.err.println("No parquet files found under " + root);
            System.exit(1);
        }

        long t0 = System.currentTimeMillis();
        int filesDone = 0, filesSkipped = 0;
        long rowsWritten = 0;
        for (Path p : parquets) {
            filesDone++;
            long ts0 = System.currentTimeMillis();
            long written = processOne(p.toFile(), exchange, baseDir, skipExisting);
            long ts1 = System.currentTimeMillis();
            rowsWritten += written;
            if (written == 0 && skipExisting) filesSkipped++;
            System.out.printf("[%d/%d] %s → %,d rows in %d ms%n",
                    filesDone, parquets.size(), p.getFileName(), written, ts1 - ts0);
        }
        long t2 = System.currentTimeMillis();
        System.out.printf("[klines-backfill] DONE: %d parquets (%d skipped), %,d total rows, %d ms%n",
                parquets.size(), filesSkipped, rowsWritten, t2 - t0);
    }

    private static List<Path> discoverParquets(Path root, java.time.LocalDate from, java.time.LocalDate to) throws IOException {
        List<Path> out = new ArrayList<>();
        if (Files.isRegularFile(root)) {
            out.add(root);
            return out;
        }
        try (Stream<Path> s = Files.walk(root)) {
            s.filter(p -> {
                String n = p.getFileName().toString();
                if (!n.startsWith("klines_") || !n.endsWith(".parquet")) return false;
                if (from == null && to == null) return true;
                java.time.LocalDate day = dayFromFilename(n);
                if (day == null) return true;
                if (from != null && day.isBefore(from)) return false;
                if (to != null && day.isAfter(to)) return false;
                return true;
            }).sorted().forEach(out::add);
        }
        return out;
    }

    private static java.time.LocalDate dayFromFilename(String filename) {
        // e.g. klines_2026-04-19_h07.parquet
        int u1 = filename.indexOf('_');
        int u2 = filename.indexOf('_', u1 + 1);
        if (u1 < 0 || u2 <= u1 + 10) return null;
        try {
            return java.time.LocalDate.parse(filename.substring(u1 + 1, u1 + 11));
        } catch (Exception e) {
            return null;
        }
    }

    private static long processOne(File parquetFile, String exchange, Path baseDir, boolean skipExisting) throws IOException {
        java.time.LocalDate day = dayFromFilename(parquetFile.getName());
        int hourFromFilename = hourFromFilename(parquetFile.getName());

        Map<String, List<Row>> byInstrument = readParquetGrouped(parquetFile);
        if (byInstrument.isEmpty()) return 0;

        long rowsWritten = 0;
        int wroteFiles = 0, skippedFiles = 0;
        for (Map.Entry<String, List<Row>> e : new TreeMap<>(byInstrument).entrySet()) {
            String instrument = e.getKey();
            List<Row> rows = e.getValue();
            if (rows.isEmpty()) continue;
            rows.sort(Comparator.comparingLong(r -> r.tsMs));

            String monthStr, dayOfMonthStr, hourStr;
            if (day != null && hourFromFilename >= 0) {
                monthStr = String.format(Locale.ROOT, "%04d-%02d", day.getYear(), day.getMonthValue());
                dayOfMonthStr = String.format(Locale.ROOT, "%02d", day.getDayOfMonth());
                hourStr = String.format(Locale.ROOT, "%02d", hourFromFilename);
            } else {
                long firstTsMs = rows.get(0).tsMs;
                ZonedDateTime zdt = Instant.ofEpochMilli(firstTsMs).atZone(ZoneOffset.UTC);
                monthStr = String.format(Locale.ROOT, "%04d-%02d", zdt.getYear(), zdt.getMonthValue());
                dayOfMonthStr = String.format(Locale.ROOT, "%02d", zdt.getDayOfMonth());
                hourStr = String.format(Locale.ROOT, "%02d", zdt.getHour());
            }

            // Namespace klines under a `klines/` subdir so we don't collide with
            // the tickers writer which targets `{base}/{exch}/{ins}/YYYY-MM/DD/h{HH}.lastra`
            // — that way existing tickers readers ignore kline files and the
            // --skip-existing flag only matches actual prior kline output.
            Path dir = baseDir.resolve(exchange).resolve(instrument.replace('/', '_'))
                    .resolve(monthStr).resolve(dayOfMonthStr).resolve("klines");
            Path finalPath = dir.resolve("h" + hourStr + ".lastra");
            if (skipExisting && Files.exists(finalPath)) {
                skippedFiles++;
                continue;
            }
            Files.createDirectories(dir);
            Path tmpPath = dir.resolve("h" + hourStr + ".lastra.tmp");
            writeLastra(tmpPath, rows);
            Files.move(tmpPath, finalPath, StandardCopyOption.REPLACE_EXISTING);
            wroteFiles++;
            rowsWritten += rows.size();
        }
        if (skippedFiles > 0 && wroteFiles == 0) return 0;
        return rowsWritten;
    }

    private static int hourFromFilename(String filename) {
        int i = filename.indexOf("_h");
        if (i < 0) return -1;
        try {
            return Integer.parseInt(filename.substring(i + 2, i + 4));
        } catch (Exception e) {
            return -1;
        }
    }

    private static Map<String, List<Row>> readParquetGrouped(File parquetFile) throws IOException {
        List<String> needed = new ArrayList<>();
        needed.add("ins");
        needed.add("t");
        needed.addAll(Arrays.asList(DOUBLE_COLS));
        needed.add("ntr");

        Map<String, List<Row>> out = new HashMap<>();
        try (Stream<Map<String, Object>> stream = ParquetReader.streamContent(
                parquetFile, new MapHydratorSupplier(), needed)) {
            stream.forEach(m -> {
                Object ins = m.get("ins");
                String insStr = ins == null ? null : ins.toString();
                if (insStr == null || insStr.isEmpty()) return;
                Row r = new Row();
                Object t = m.get("t");
                // QDB parquet stores t as INT64 microseconds since epoch — convert to ms.
                r.tsMs = ((Number) t).longValue() / 1000L;
                for (int k = 0; k < DOUBLE_COLS.length; k++) {
                    Object v = m.get(DOUBLE_COLS[k]);
                    r.values[k] = v == null ? 0.0 : ((Number) v).doubleValue();
                }
                Object ntr = m.get("ntr");
                r.ntr = ntr == null ? 0L : ((Number) ntr).longValue();
                out.computeIfAbsent(insStr, __ -> new ArrayList<>()).add(r);
            });
        }
        return out;
    }

    private static void writeLastra(Path tmpPath, List<Row> rows) throws IOException {
        int n = rows.size();
        long[] ts = new long[n];
        double[] opn = new double[n];
        double[] hig = new double[n];
        double[] low = new double[n];
        double[] cls = new double[n];
        double[] vol = new double[n];
        double[] vlq = new double[n];
        long[] ntr = new long[n];
        for (int i = 0; i < n; i++) {
            Row r = rows.get(i);
            ts[i] = r.tsMs;
            opn[i] = r.values[0];
            hig[i] = r.values[1];
            low[i] = r.values[2];
            cls[i] = r.values[3];
            vol[i] = r.values[4];
            vlq[i] = r.values[5];
            ntr[i] = r.ntr;
        }
        try (FileOutputStream fos = new FileOutputStream(tmpPath.toFile());
             LastraWriter w = new LastraWriter(fos)) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("opn", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addSeriesColumn("hig", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addSeriesColumn("low", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addSeriesColumn("cls", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addSeriesColumn("vol", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addSeriesColumn("vlq", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            // Trade count — small, monotonic within session — compresses well with DELTA_VARINT.
            w.addSeriesColumn("ntr", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.writeSeries(n, ts, opn, hig, low, cls, vol, vlq, ntr);
            w.close();
            fos.getFD().sync();
        }
    }

    private static final class Row {
        long tsMs;
        final double[] values = new double[DOUBLE_COLS.length];
        long ntr;
    }

    private static final class MapHydratorSupplier
            implements HydratorSupplier<Map<String, Object>, Map<String, Object>> {
        @Override
        public Hydrator<Map<String, Object>, Map<String, Object>> get(
                List<org.apache.parquet.column.ColumnDescriptor> columns) {
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
