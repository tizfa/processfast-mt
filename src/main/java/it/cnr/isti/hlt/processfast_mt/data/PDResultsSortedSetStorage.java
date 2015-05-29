package it.cnr.isti.hlt.processfast_mt.data;

import java.io.Serializable;
import java.util.Collection;

/**
 * A storage containing PD temporary results in form of a sorted set. Every implementation must be
 * thread-safe.
 *
 * @param < T >   The type of data stored. The specified type must also implements {@link Comparable} interface.
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public interface PDResultsSortedSetStorage<K extends Comparable & Serializable, V extends Serializable> extends Serializable {
    /**
     * Get the storage ID.
     *
     * @return The storage ID.
     */
    String getStorageID();

    /**
     * Indicate if the set ids ordered ascending or descending.
     *
     * @return True if the set is ordered ascending, false otherwise.
     */
    boolean isSortedAscending();

    /**
     * Add a set of results to storage.
     *
     * @param c The set of results to be added.
     */
    void addResults(Collection<SortedSetItem<K, V>> c);

    /**
     * Get a set of results from storage.
     *
     * @param startIdx The start idx.
     * @param endIdx   The end idx (excluded).
     * @return The requested set of results.
     */
    Collection<V> getResults(long startIdx, long endIdx);

    /**
     * Get the number of results available.
     *
     * @return The number of results available.
     */
    long size();

    /**
     * Check if the specified item is contained in this
     * storage.
     *
     * @param item
     * @return True if the item is contained in this storage, false otherwise.
     */
    boolean contains(SortedSetItem<K, V> item);
}
