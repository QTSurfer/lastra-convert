package com.wualabs.qtsurfer.reef.convert;

import com.wualabs.qtsurfer.reef.Reef;
import com.wualabs.qtsurfer.reef.Reef.Codec;
import com.wualabs.qtsurfer.reef.Reef.DataType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CLI for converting Parquet files to Reef format.
 *
 * <p>Usage:
 * <pre>
 *   reef-convert &lt;input.parquet&gt; [output.reef] [options]
 *
 *   Options:
 *     --columns COL:TYPE:CODEC,...   Column mappings (default: auto-detect from schema)
 *     --inspect                      Show Parquet schema and exit
 *
 *   Types:  long, double, binary
 *   Codecs: delta_varint, alp, raw, varlen, varlen_zstd, varlen_gzip
 *
 *   Examples:
 *     reef-convert data.parquet
 *     reef-convert data.parquet output.reef
 *     reef-convert data.parquet --columns t:long:delta_varint,close:double:alp
 *     reef-convert data.parquet --inspect
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

        for (int i = 1; i < args.length; i++) {
            if ("--inspect".equals(args[i])) {
                inspect = true;
            } else if ("--columns".equals(args[i]) && i + 1 < args.length) {
                columnsArg = args[++i];
            } else if (!args[i].startsWith("--")) {
                outputPath = args[i];
            }
        }

        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.err.println("Error: file not found: " + inputPath);
            System.exit(1);
        }

        if (inspect) {
            InspectParquet.main(new String[]{inputPath});
            return;
        }

        // Default output: same name with .reef extension
        if (outputPath == null) {
            String name = inputFile.getName();
            int dot = name.lastIndexOf('.');
            String base = dot > 0 ? name.substring(0, dot) : name;
            outputPath = new File(inputFile.getParentFile(), base + ".reef").getPath();
        }

        // Build converter
        ParquetToReefConverter.Builder builder = ParquetToReefConverter.builder(inputFile);

        if (columnsArg != null) {
            parseColumns(columnsArg, builder);
        } else {
            autoDetectColumns(inputFile, builder);
        }

        ParquetToReefConverter converter = builder.build();

        File outputFile = new File(outputPath);
        try (FileOutputStream out = new FileOutputStream(outputFile)) {
            int rows = converter.convert(out);
            long reefSize = outputFile.length();
            long parquetSize = inputFile.length();
            double ratio = parquetSize > 0 ? (double) parquetSize / reefSize : 0;

            System.out.printf("Converted %,d rows → %s (%,d bytes, %.1fx compression vs parquet)%n",
                    rows, outputFile.getName(), reefSize, ratio);
        }
    }

    private static void parseColumns(String columnsArg, ParquetToReefConverter.Builder builder) {
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

    private static void autoDetectColumns(File parquetFile, ParquetToReefConverter.Builder builder) throws IOException {
        var metadata = com.wualabs.qtsurfer.parquet.ParquetReader.readMetadata(parquetFile);
        var schema = metadata.getFileMetaData().getSchema();

        for (var col : schema.getColumns()) {
            String name = col.getPath()[0];
            var parquetType = col.getPrimitiveType().getPrimitiveTypeName();

            DataType dataType;
            Codec codec;

            switch (parquetType) {
                case INT64:
                case INT32:
                    dataType = DataType.LONG;
                    codec = Codec.DELTA_VARINT;
                    break;
                case DOUBLE:
                case FLOAT:
                    dataType = DataType.DOUBLE;
                    codec = Codec.ALP;
                    break;
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
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
        System.out.println("Usage: reef-convert <input.parquet> [output.reef] [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --columns COL:TYPE:CODEC,...   Column mappings (default: auto-detect)");
        System.out.println("  --inspect                      Show Parquet schema and exit");
        System.out.println();
        System.out.println("Types:  long, double, binary");
        System.out.println("Codecs: delta_varint, alp, raw, varlen, varlen_zstd, varlen_gzip");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  reef-convert data.parquet");
        System.out.println("  reef-convert data.parquet output.reef");
        System.out.println("  reef-convert data.parquet --columns t:long:delta_varint,close:double:alp");
    }
}
