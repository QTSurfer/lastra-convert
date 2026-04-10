package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.lastra.Lastra;
import com.wualabs.qtsurfer.lastra.Lastra.Codec;
import com.wualabs.qtsurfer.lastra.Lastra.DataType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * CLI for converting Parquet files to Lastra format.
 *
 * <p>Usage:
 * <pre>
 *   lastra-convert &lt;input.parquet&gt; [output.lastra] [options]
 *
 *   Options:
 *     --columns COL:TYPE:CODEC,...   Column mappings (default: auto-detect, ALP for doubles)
 *     --smart                        Auto-select best codec per column (sample-based, fast)
 *     --best                         Try all codecs per column, pick smallest (slower, optimal)
 *     --inspect                      Show Parquet schema and exit
 *
 *   Types:  long, double, binary
 *   Codecs: delta_varint, alp, gorilla, pongo, raw, varlen, varlen_zstd, varlen_gzip
 * </pre>
 */
public final class Main {

    private static final Map<String, DataType> TYPE_MAP = new LinkedHashMap<>();
    private static final Map<String, Codec> CODEC_MAP = new LinkedHashMap<>();

    static {
        TYPE_MAP.put("long", DataType.LONG);
        TYPE_MAP.put("double", DataType.DOUBLE);
        TYPE_MAP.put("binary", DataType.BINARY);

        CODEC_MAP.put("delta_varint", Codec.DELTA_VARINT);
        CODEC_MAP.put("alp", Codec.ALP);
        CODEC_MAP.put("gorilla", Codec.GORILLA);
        CODEC_MAP.put("pongo", Codec.PONGO);
        CODEC_MAP.put("raw", Codec.RAW);
        CODEC_MAP.put("varlen", Codec.VARLEN);
        CODEC_MAP.put("varlen_zstd", Codec.VARLEN_ZSTD);
        CODEC_MAP.put("varlen_gzip", Codec.VARLEN_GZIP);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = null;
        String columnsArg = null;
        boolean inspect = false;
        boolean smart = false;
        boolean best = false;

        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--inspect": inspect = true; break;
                case "--smart": smart = true; break;
                case "--best": best = true; break;
                case "--columns":
                    if (i + 1 < args.length) columnsArg = args[++i];
                    break;
                default:
                    if (!args[i].startsWith("--")) outputPath = args[i];
            }
        }

        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.err.println("Error: file not found: " + inputPath);
            System.exit(1);
        }

        // Detect format by input file extension
        String inputLower = inputPath.toLowerCase();
        boolean isLastraInput = inputLower.endsWith(".lastra");
        boolean isCsvInput = inputLower.endsWith(".csv") || inputLower.endsWith(".tsv");

        if (inspect) {
            if (isLastraInput) {
                LastraToParquetConverter.inspect(inputFile);
            } else if (isCsvInput) {
                System.out.println("CSV inspect not supported. Use --inspect on .parquet or .lastra files.");
            } else {
                InspectParquet.main(new String[]{inputPath});
            }
            return;
        }

        // Determine output extension
        if (outputPath == null) {
            String name = inputFile.getName();
            int dot = name.lastIndexOf('.');
            String base = dot > 0 ? name.substring(0, dot) : name;
            String ext;
            if (isLastraInput) {
                // Lastra → default to Parquet, or CSV if output ends with .csv
                ext = ".parquet";
            } else {
                // Parquet/CSV → Lastra
                ext = ".lastra";
            }
            outputPath = new File(inputFile.getParentFile(), base + ext).getPath();
        }

        File outputFile = new File(outputPath);
        boolean outputCsv = outputPath.toLowerCase().endsWith(".csv")
                || outputPath.toLowerCase().endsWith(".tsv");

        if (isLastraInput && outputCsv) {
            // Lastra → CSV
            LastraToCsvConverter converter = new LastraToCsvConverter(inputFile);
            try (FileOutputStream out = new FileOutputStream(outputFile)) {
                int rows = converter.convert(out);
                printResult(inputFile, outputFile, rows);
            }
        } else if (isLastraInput) {
            // Lastra → Parquet
            LastraToParquetConverter converter = new LastraToParquetConverter(inputFile);
            try (FileOutputStream out = new FileOutputStream(outputFile)) {
                int rows = converter.convert(out);
                printResult(inputFile, outputFile, rows);
            }
        } else if (isCsvInput) {
            // CSV → Lastra
            CsvToLastraConverter converter = new CsvToLastraConverter(inputFile);
            try (FileOutputStream out = new FileOutputStream(outputFile)) {
                int rows = converter.convert(out);
                printResult(inputFile, outputFile, rows);
            }
        } else if (smart || best) {
            // Parquet → Lastra (smart/best mode)
            SmartParquetToLastraConverter.Mode mode = best
                    ? SmartParquetToLastraConverter.Mode.BEST
                    : SmartParquetToLastraConverter.Mode.SMART;

            SmartParquetToLastraConverter converter = SmartParquetToLastraConverter.create(inputFile, mode);

            try (FileOutputStream out = new FileOutputStream(outputFile)) {
                int rows = converter.convert(out);
                printResult(inputFile, outputFile, rows);
            }
        } else {
            // Parquet → Lastra (standard mode)
            ParquetToLastraConverter.Builder builder = ParquetToLastraConverter.builder(inputFile);

            if (columnsArg != null) {
                parseColumns(columnsArg, builder);
            } else {
                autoDetectColumns(inputFile, builder);
            }

            ParquetToLastraConverter converter = builder.build();

            try (FileOutputStream out = new FileOutputStream(outputFile)) {
                int rows = converter.convert(out);
                printResult(inputFile, outputFile, rows);
            }
        }
    }

    private static void printResult(File inputFile, File outputFile, int rows) {
        long lastraSize = outputFile.length();
        long parquetSize = inputFile.length();
        double ratio = parquetSize > 0 ? (double) parquetSize / lastraSize : 0;
        System.out.printf("%nConverted %,d rows → %s (%,d bytes, %.1fx compression vs parquet)%n",
                rows, outputFile.getName(), lastraSize, ratio);
    }

    private static void parseColumns(String columnsArg, ParquetToLastraConverter.Builder builder) {
        for (String spec : columnsArg.split(",")) {
            String[] parts = spec.trim().split(":");
            if (parts.length < 3) {
                System.err.println("Error: invalid column spec '" + spec + "', expected NAME:TYPE:CODEC");
                System.exit(1);
            }
            String name = parts[0].trim();
            DataType type = TYPE_MAP.get(parts[1].trim().toLowerCase());
            Codec codec = CODEC_MAP.get(parts[2].trim().toLowerCase());
            if (type == null) {
                System.err.println("Error: unknown type '" + parts[1] + "'. Valid: " + TYPE_MAP.keySet());
                System.exit(1);
            }
            if (codec == null) {
                System.err.println("Error: unknown codec '" + parts[2] + "'. Valid: " + CODEC_MAP.keySet());
                System.exit(1);
            }
            builder.map(name, type, codec);
        }
    }

    private static void autoDetectColumns(File parquetFile, ParquetToLastraConverter.Builder builder) throws IOException {
        var metadata = com.wualabs.qtsurfer.parquet.ParquetReader.readMetadata(parquetFile);
        var schema = metadata.getFileMetaData().getSchema();

        for (var col : schema.getColumns()) {
            String name = col.getPath()[0];
            var parquetType = col.getPrimitiveType().getPrimitiveTypeName();

            DataType dataType;
            Codec codec;

            switch (parquetType) {
                case INT64: case INT32:
                    dataType = DataType.LONG;
                    codec = Codec.DELTA_VARINT;
                    break;
                case DOUBLE: case FLOAT:
                    dataType = DataType.DOUBLE;
                    codec = Codec.ALP;
                    break;
                case BINARY: case FIXED_LEN_BYTE_ARRAY:
                    dataType = DataType.BINARY;
                    codec = Codec.VARLEN_ZSTD;
                    break;
                case BOOLEAN:
                    dataType = DataType.LONG;
                    codec = Codec.RAW;
                    break;
                default:
                    System.err.println("Warning: skipping unsupported column '" + name + "' (" + parquetType + ")");
                    continue;
            }

            System.out.printf("  %s → %s / %s%n", name, dataType, codec);
            builder.map(name, dataType, codec);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: lastra-convert <input> [output] [options]");
        System.out.println();
        System.out.println("Formats (auto-detected by extension):");
        System.out.println("  .parquet/.pqt → Lastra     .csv/.tsv → Lastra");
        System.out.println("  .lastra → Parquet           .lastra → CSV (if output is .csv)");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --columns COL:TYPE:CODEC,...   Column mappings (Parquet/CSV→Lastra)");
        System.out.println("  --smart                        Auto-select best codec per column (fast)");
        System.out.println("  --best                         Try all codecs per column (optimal)");
        System.out.println("  --inspect                      Show file structure and exit");
        System.out.println();
        System.out.println("Types:  long, double, binary");
        System.out.println("Codecs: delta_varint, alp, gorilla, pongo, raw, varlen, varlen_zstd, varlen_gzip");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  lastra-convert data.parquet                      # Parquet → Lastra");
        System.out.println("  lastra-convert data.parquet --best               # Parquet → Lastra (optimal)");
        System.out.println("  lastra-convert data.csv                          # CSV → Lastra");
        System.out.println("  lastra-convert data.lastra                         # Lastra → Parquet");
        System.out.println("  lastra-convert data.lastra output.csv              # Lastra → CSV");
        System.out.println("  lastra-convert data.lastra --inspect               # Inspect Lastra file");
    }
}
