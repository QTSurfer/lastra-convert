package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.lastra.Lastra;

import java.util.Map;

/**
 * Maps a source column to a Lastra series column with its data type and codec.
 */
public final class ColumnMapping {

    private final String sourceName;
    private final String lastraName;
    private final Lastra.DataType dataType;
    private final Lastra.Codec codec;
    private final Map<String, String> metadata;

    public ColumnMapping(String sourceName, String lastraName, Lastra.DataType dataType, Lastra.Codec codec) {
        this(sourceName, lastraName, dataType, codec, null);
    }

    public ColumnMapping(String sourceName, String lastraName, Lastra.DataType dataType, Lastra.Codec codec,
                         Map<String, String> metadata) {
        this.sourceName = sourceName;
        this.lastraName = lastraName;
        this.dataType = dataType;
        this.codec = codec;
        this.metadata = metadata;
    }

    public String sourceName() { return sourceName; }
    public String lastraName() { return lastraName; }
    public Lastra.DataType dataType() { return dataType; }
    public Lastra.Codec codec() { return codec; }
    public Map<String, String> metadata() { return metadata; }

    /**
     * Shorthand: same name for source and lastra, no metadata.
     */
    public static ColumnMapping of(String name, Lastra.DataType dataType, Lastra.Codec codec) {
        return new ColumnMapping(name, name, dataType, codec);
    }

    /**
     * Shorthand with rename.
     */
    public static ColumnMapping of(String sourceName, String lastraName, Lastra.DataType dataType, Lastra.Codec codec) {
        return new ColumnMapping(sourceName, lastraName, dataType, codec);
    }
}
