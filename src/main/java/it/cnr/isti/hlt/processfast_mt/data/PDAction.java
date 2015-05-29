package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.CacheType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public interface PDAction<Out> {
    /**
     * Apply the action on a source collection.
     *
     * @param source
     * @return
     */
    Out applyAction(Stream source);

    /**
     * Merge the results in "src" to the results in "dest".
     *
     * @param storageManager The storage manager available.
     * @param src            The source results.
     * @param dest           The destination results or 'null' if there are no results in dest.
     * @param cacheType      The type of cache where to merge results. Used when dest is 'null' and a new
     *                       structure for store intermediate results needs to be created.
     * @return The merged results.
     */
    void mergeResults(PDResultsStorageManager storageManager, Out src, Map dest, CacheType cacheType);

    /**
     * Indicate if the action need more results to compute the
     * desired behaviour.
     *
     * @param currentResults The current results.
     * @return True if the action need more results, false otherwise.
     */
    boolean needMoreResults(Map currentResults);

    /**
     * Get the final results to be returned to action caller by
     * translating the specified final internal results.
     *
     * @param internalResults The computed final internal results.
     * @return The final results for action caller.
     */
    Out getFinalResults(PDResultsStorageManager storageManager, Map internalResults);
}

