package it.cnr.isti.hlt.processfast_java8.data;

import it.cnr.isti.hlt.processfast.data.CacheType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Record handle for a real partitionable dataset transformation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public interface PDTransformation extends PDBaseTransformation {
    /**
     * Apply a specific transformation on source collection.
     *
     * @param source The source data collection.
     * @return The resulting collection after the operation has been applied.
     */
    Stream applyTransformation(Stream source);

    /**
     * Indicate if the transformation need all available data or just some data chunk
     * to perform correctly.
     *
     * @return True if alla the data is necessary, false otherwise.
     */
    boolean needAllAvailableData();

    /**
     * Merge the results in "src" to the results in "dest".
     *
     * @param storageManager The storage manager to use.
     * @param src            The source results.
     * @param dest           The destination results or 'null' if there are no results in dest.
     * @param cacheType      The type of cache where to merge results. Used when dest is 'null' and a new
     *                       structure for store intermediate results needs to be created.
     * @return The merged results.
     */
    void mergeResults(PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType);

    /**
     * Get the final results to be returned to action caller by
     * translating the specified final internal results.
     *
     * @param internalResults The computed final internal results.
     * @return The final results for action caller.
     */
    PDResultsStorageIteratorProvider getFinalResults(Map internalResults);
}

