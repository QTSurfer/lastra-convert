package com.wualabs.qtsurfer.reef.convert;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Converts a source data format into a Reef file.
 */
public interface ReefConverter {

    /**
     * Converts the source and writes the Reef output to the given stream.
     *
     * @param out destination stream for the Reef file
     * @return number of rows written
     */
    int convert(OutputStream out) throws IOException;
}
