package com.wualabs.qtsurfer.lastra.convert;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Converts a source data format into a Lastra file.
 */
public interface LastraConverter {

    /**
     * Converts the source and writes the Lastra output to the given stream.
     *
     * @param out destination stream for the Lastra file
     * @return number of rows written
     */
    int convert(OutputStream out) throws IOException;
}
