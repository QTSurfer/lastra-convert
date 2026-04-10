package com.wualabs.qtsurfer.lastra.convert;

import com.wualabs.qtsurfer.lastra.Lastra.Codec;
import com.wualabs.qtsurfer.lastra.codec.GorillaCodec;
import com.wualabs.qtsurfer.lastra.codec.PongoCodec;
import com.wualabs.qtsurfer.alp.AlpCompressor;
import com.wualabs.qtsurfer.lastra.codec.RawCodec;

/**
 * Selects the best Lastra codec for a double column by trial-encoding.
 *
 * <p>Supports two modes:
 * <ul>
 *   <li><b>Sample</b> ({@link #selectBySample}): encodes first N values with each codec, picks smallest.
 *       Fast, good for auto-detect.</li>
 *   <li><b>Full</b> ({@link #selectByFullScan}): encodes all values with each codec, picks smallest.
 *       Slower but optimal.</li>
 * </ul>
 */
public final class CodecSelector {

    private static final Codec[] DOUBLE_CODECS = {Codec.ALP, Codec.GORILLA, Codec.PONGO};
    private static final int DEFAULT_SAMPLE_SIZE = 512;

    private CodecSelector() {}

    /**
     * Select best codec by encoding a sample of the data.
     */
    public static Result selectBySample(double[] values, int count) {
        int sampleSize = Math.min(count, DEFAULT_SAMPLE_SIZE);
        double[] sample;
        if (sampleSize == count) {
            sample = values;
        } else {
            sample = new double[sampleSize];
            System.arraycopy(values, 0, sample, 0, sampleSize);
        }
        return selectByFullScan(sample, sampleSize);
    }

    /**
     * Select best codec by encoding all data with each codec and picking the smallest.
     */
    public static Result selectByFullScan(double[] values, int count) {
        Codec bestCodec = Codec.ALP;
        int bestSize = Integer.MAX_VALUE;
        int[] sizes = new int[DOUBLE_CODECS.length];

        for (int i = 0; i < DOUBLE_CODECS.length; i++) {
            byte[] encoded = encodeWith(values, count, DOUBLE_CODECS[i]);
            sizes[i] = encoded.length;
            if (encoded.length < bestSize) {
                bestSize = encoded.length;
                bestCodec = DOUBLE_CODECS[i];
            }
        }

        return new Result(bestCodec, sizes);
    }

    private static byte[] encodeWith(double[] values, int count, Codec codec) {
        switch (codec) {
            case ALP: return new AlpCompressor().compress(values, count);
            case GORILLA: return GorillaCodec.encode(values, count);
            case PONGO: return PongoCodec.encode(values, count);
            default: return RawCodec.encodeDoubles(values, count);
        }
    }

    public static final class Result {
        private final Codec bestCodec;
        private final int[] sizes; // [ALP, GORILLA, PONGO]

        Result(Codec bestCodec, int[] sizes) {
            this.bestCodec = bestCodec;
            this.sizes = sizes;
        }

        public Codec bestCodec() { return bestCodec; }

        public String report() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < DOUBLE_CODECS.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(DOUBLE_CODECS[i].name()).append("=").append(formatSize(sizes[i]));
                if (DOUBLE_CODECS[i] == bestCodec) sb.append("*");
            }
            return sb.toString();
        }

        private static String formatSize(int bytes) {
            if (bytes < 1024) return bytes + "B";
            return String.format("%.1fKB", bytes / 1024.0);
        }
    }
}
