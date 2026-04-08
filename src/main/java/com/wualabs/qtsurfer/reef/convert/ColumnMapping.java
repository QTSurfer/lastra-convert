package com.wualabs.qtsurfer.reef.convert;

import com.wualabs.qtsurfer.reef.Reef;

import java.util.Map;

/**
 * Maps a source column to a Reef series column with its data type and codec.
 */
public final class ColumnMapping {

    private final String sourceName;
    private final String reefName;
    private final Reef.DataType dataType;
    private final Reef.Codec codec;
    private final Map<String, String> metadata;

    public ColumnMapping(String sourceName, String reefName, Reef.DataType dataType, Reef.Codec codec) {
        this(sourceName, reefName, dataType, codec, null);
    }

    public ColumnMapping(String sourceName, String reefName, Reef.DataType dataType, Reef.Codec codec,
                         Map<String, String> metadata) {
        this.sourceName = sourceName;
        this.reefName = reefName;
        this.dataType = dataType;
        this.codec = codec;
        this.metadata = metadata;
    }

    public String sourceName() { return sourceName; }
    public String reefName() { return reefName; }
    public Reef.DataType dataType() { return dataType; }
    public Reef.Codec codec() { return codec; }
    public Map<String, String> metadata() { return metadata; }

    /**
     * Shorthand: same name for source and reef, no metadata.
     */
    public static ColumnMapping of(String name, Reef.DataType dataType, Reef.Codec codec) {
        return new ColumnMapping(name, name, dataType, codec);
    }

    /**
     * Shorthand with rename.
     */
    public static ColumnMapping of(String sourceName, String reefName, Reef.DataType dataType, Reef.Codec codec) {
        return new ColumnMapping(sourceName, reefName, dataType, codec);
    }
}
