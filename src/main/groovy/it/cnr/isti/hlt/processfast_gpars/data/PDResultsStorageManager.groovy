/*
 * *****************
 *  Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * *******************
 */

package it.cnr.isti.hlt.processfast_gpars.data

import groovy.transform.CompileStatic
import it.cnr.isti.hlt.processfast.data.CacheType


@CompileStatic
interface PDResultsStorageManagerProvider {

    /**
     * Create a new storage manager with the specified ID.
     *
     * @param storageManagerID The ID of the storage manager to create.
     * @return The new created storage manager.
     */
    PDResultsStorageManager createStorageManager(String storageManagerID)

    /**
     * Delete the specified storage manager ID.
     *
     * @param storageManagerID
     */
    void deleteStorageManager(String storageManagerID)

    /**
     * Generate unique storage manager ID in the provider.
     *
     * @return An unique storage ID.
     */
    String generateUniqueStorageManagerID()
}

/**
 * A generic storage manager for intermediate results in PDs operations. Every
 * implementation must be thread safe.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
interface PDResultsStorageManager {

    /**
     * Get the storage manager ID.
     *
     * @return The storage manager ID.
     */
    String getStorageManagerID()

    /**
     * Create a storage containing PD results in the form of a unordered data collection. If the storage does not exist, it will
     * be created. If the storage exists, it will be returned.
     *
     * @param storageID The storage ID.
     * @param cacheType Indicate where to store the storage.
     * @return The requested storage ID.
     */
    public <T extends Serializable> PDResultsCollectionStorage<T> createCollectionStorage(String storageID, CacheType cacheType)

    /**
     * Delete the collection storage with the given ID.
     *
     * @param storageID The storage ID.
     */
    public <T extends Serializable> void deleteCollectionStorage(String storageID)

    /**
     * Create a storage containing PD results in the form of a sorted data set. If the storage does not exist, it will
     * bne created. If the storage exists, it will be returned.
     *
     * @param storageID The storage ID.
     * @param cacheType Indicate where to store the storage.
     * @param sortAscending True if the data must be ordered ascending, false if must be ordered descending.
     * @return The requested storage ID.
     */
    public <K extends Comparable & Serializable, V extends Serializable> PDResultsSortedSetStorage<K, V> createSortedSetStorage(String storageID, CacheType cacheType, boolean sortAscending)

    /**
     * Delete the sorted storage with the given ID.
     *
     * @param storageID The storage ID.
     */
    public <T extends Serializable> void deleteSortedSetStorage(String storageID)

    /**
     * Create a storage containing PD results in the form of unordered dictionary data. If the storage does not exist, it will
     * bne created. If the storage exists, it will be returned.
     *
     * @param storageID The storage ID.
     * @param cacheType Indicate where to store the storage.
     * @return The requested storage ID.
     */
    public <K extends Serializable, V extends Serializable> PDResultsMapStorage<K, V> createMapStorage(String storageID, CacheType cacheType)

    /**
     * Delete the map storage with the given ID.
     *
     * @param storageID The storage ID.
     */
    public <T extends Serializable> void deleteMapStorage(String storageID)

    /**
     * Generate unique storage ID in the storage manager.
     *
     * @return An unique storage ID.
     */
    String generateUniqueStorageID()
}

/**
 * A storage containing PD temporary results in form of a collection of data. Every implementation must be
 * thread-safe.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
interface PDResultsCollectionStorage<T extends Serializable> extends Serializable {

    /**
     * Remove all stored results.
     */
    void clear()

    /**
     * Get the storage ID.
     * @return The storage ID.
     */
    String getStorageID()

    /**
     * Add a set of results to storage.
     *
     * @param c
     */
    void addResults(Collection<T> c);

    /**
     * Get a set of results from storage.
     *
     * @param startIdx The start idx.
     * @param endIdx The end idx (excluded).
     * @return The requested set of results.
     */
    Collection<T> getResults(long startIdx, long endIdx)

    /**
     * Get the number of results available.
     *
     * @return The number of results available.
     */
    long size()

    /**
     * Check if the specified item is contained in this
     * storage.
     *
     * @param item
     * @return True if the item is contained in this storage, false otherwise.
     */
    boolean contains(T item)
}

/**
 * A storage containing PD temporary results in form of a sorted set. Every implementation must be
 * thread-safe.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 * @param < T >   The type of data stored. The specified type must also implements {@link Comparable} interface.
 */
@CompileStatic
interface PDResultsSortedSetStorage<K extends Comparable & Serializable, V extends Serializable> extends Serializable {

    /**
     * Get the storage ID.
     * @return The storage ID.
     */
    String getStorageID()

    /**
     * Indicate if the set ids ordered ascending or descending.
     *
     * @return True if the set is ordered ascending, false otherwise.
     */
    boolean isSortedAscending()

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
     * @param endIdx The end idx (excluded).
     * @return The requested set of results.
     */
    Collection<V> getResults(long startIdx, long endIdx)

    /**
     * Get the number of results available.
     *
     * @return The number of results available.
     */
    long size()

    /**
     * Check if the specified item is contained in this
     * storage.
     *
     * @param item
     * @return True if the item is contained in this storage, false otherwise.
     */
    boolean contains(SortedSetItem<K, V> item)
}

@CompileStatic
class SortedSetItem<K extends Comparable & Serializable, V extends Serializable> implements Comparable<SortedSetItem<K, V>> {
    /**
     * The key used for comparison purposes.
     */
    K key

    /**
     * The associated stored item.
     */
    V item

    SortedSetItem(K key, V item) {
        if (key == null)
            throw new NullPointerException("The key value is 'null'")
        if (item == null)
            throw new IllegalArgumentException("The item is 'null'")
        this.key = key
        this.item = item
    }

    @Override
    boolean equals(Object obj) {
        if (!obj instanceof SortedSetItem)
            return false
        SortedSetItem si = (SortedSetItem) obj
        return key.equals(si.key) && item.equals(si.item)
    }


    @Override
    int compareTo(SortedSetItem o) {
        int ret = key.compareTo(o.key)
        if (ret == 0)
        // Prevent items to be excluded because the key is the same but are different objects.
            ret = -1
        return ret
    }
}

/**
 * A storage containing PD temporary results in forms of a dictionary. Every implementation must be
 * thread-safe.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
interface PDResultsMapStorage<Key extends Serializable, V extends Serializable> extends Serializable {

    /**
     * Get the storage ID.
     * @return The storage ID.
     */
    String getStorageID()

    /**
     * Remove the item with specified key.
     *
     * @param key The key of the item to be removed.
     */
    void remove(Key key)

    /**
     * Get the value associated with the specified key.
     *
     * @param key The key to search.
     * @return The requested value or 'null' if the value can not be found.
     */
    V get(Key key)

    /**
     * Associate to specified key the given value.
     *
     * @param key The key value.
     * @param value The value associated.
     */
    void put(Key key, V value)

    /**
     * Get an iterator over the set of stored keys.
     *
     * @return An iterator over the set of stored keys.
     */
    Iterator<Key> keys();

    /**
     * Get the number of results available.
     *
     * @return The number of results available.
     */
    long size()

    /**
     * Check if the specified item is contained in this
     * storage.
     *
     * @param item
     * @return True if the item is contained in this storage, false otherwise.
     */
    boolean containsKey(Key key)
}