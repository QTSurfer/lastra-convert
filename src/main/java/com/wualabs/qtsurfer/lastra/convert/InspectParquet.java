package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.parquet.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.File;

/** Quick schema inspector — run with: mvn exec:java -Dexec.mainClass=...InspectParquet -Dexec.args=FILE */
public final class InspectParquet {
    public static void main(String[] args) throws Exception {
        File f = new File(args[0]);
        ParquetMetadata meta = ParquetReader.readMetadata(f);
        MessageType schema = meta.getFileMetaData().getSchema();
        System.out.println(schema);
        System.out.println("Row groups: " + meta.getBlocks().size());
        meta.getBlocks().forEach(b -> System.out.println("  rows=" + b.getRowCount()));
    }
}
