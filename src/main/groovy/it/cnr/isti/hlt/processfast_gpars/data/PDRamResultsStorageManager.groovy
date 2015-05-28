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


/**
 * A RAM implementation of a PD results storage manager provider.
 *
 * Every created storage (indipendently if requested to be created in
 * RAM or on disk) will be created on RAM.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDRamResultsStorageManagerProvider implements PDResultsStorageManagerProvider {

    Map<String, PDRamResultsStorageManager> storages = [:]

    @Override
    synchronized PDResultsStorageManager createStorageManager(String storageManagerID) {
        if (storageManagerID == null || storageManagerID.empty)
            throw new IllegalArgumentException("The storage manager ID is 'null' or empty")
        PDResultsStorageManager storage = storages.get(storageManagerID)
        if (storage == null) {
            storage = new PDRamResultsStorageManager(storageManagerID)
            storages.put(storageManagerID, storage)
        }
        storage
    }

    @Override
    synchronized void deleteStorageManager(String storageManagerID) {
        if (storageManagerID == null || storageManagerID.empty)
            throw new IllegalArgumentException("The storage manager ID is 'null' or empty")
        storages.remove(storageManagerID)
    }

    @Override
    synchronized String generateUniqueStorageManagerID() {
        def r = new Random()
        return "${System.currentTimeMillis()}_${r.nextInt(1000000)}"
    }
}

/**
 * A RAM implementation of a PD results storage manager.
 *
 * Every created storage (indipendently if requested to be created in
 * RAM or on disk) will be created on RAM.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDRamResultsStorageManager implements PDResultsStorageManager {

    /**
     * The set of managed collection storages.
     */
    final Map<String, PDResultsCollectionStorage> collectionStorages = new HashMap<>()

    /**
     * The set of managed map storages.
     */
    final Map<String, PDRamResultsMapStorage> mapStorages = new HashMap<>()

    /**
     * The set of managed sorted set storages.
     */
    final Map<String, PDRamResultsSortedSetStorage> sortedSetStorages = new HashMap<>()

    private String storageManagerID

    PDRamResultsStorageManager(String storageManagerID) {
        if (storageManagerID == null || storageManagerID.empty)
            throw new IllegalArgumentException("The storage manager ID is 'null' or empty")
        this.storageManagerID = storageManagerID
    }


    @Override
    String getStorageManagerID() {
        this.@storageManagerID
    }

    @Override
    synchronized <T extends Serializable> PDResultsCollectionStorage<T> createCollectionStorage(String storageID, CacheType cacheType) {
        if (storageID == null || storageID.empty)
            throw new IllegalArgumentException("The storage ID is 'null' or empty")
        if (cacheType == null)
            throw new NullPointerException("The cache type is 'null'")
        PDResultsCollectionStorage st = collectionStorages.get(storageID)
        if (st == null) {
            st = new PDRamResultsCollectionStorage<T>(storageID)
            collectionStorages.put(storageID, st)
        }
        st
    }

    @Override
    synchronized <T extends Serializable> void deleteCollectionStorage(String storageID) {
        if (storageID == null || storageID.empty)
            throw new IllegalArgumentException("The storage ID is 'null' or empty")
        collectionStorages.remove(storageID)
    }

    @Override
    synchronized
    def <K extends Comparable & Serializable, V extends Serializable> PDResultsSortedSetStorage<K, V> createSortedSetStorage(String storageID, CacheType cacheType, boolean sortAscending) {
        if (storageID == null || storageID.empty)
            throw new IllegalArgumentException("The storage ID is 'null' or empty")
        if (cacheType == null)
            throw new NullPointerException("The cache type is 'null'")
        PDResultsSortedSetStorage st = sortedSetStorages.get(storageID)
        if (st == null) {
            st = new PDRamResultsSortedSetStorage<K, V>(storageID, sortAscending)
            sortedSetStorages.put(storageID, st)
        }
        st
    }

    @Override
    synchronized def <T extends Serializable> void deleteSortedSetStorage(String storageID) {
        if (storageID == null || storageID.empty)
            throw new IllegalArgumentException("The storage ID is 'null' or empty")
        sortedSetStorages.remove(storageID)
    }

    @Override
    synchronized
    def <K extends Serializable, V extends Serializable> PDResultsMapStorage<K, V> createMapStorage(String storageID, CacheType cacheType) {
        if (storageID == null || storageID.empty)
            throw new IllegalArgumentException("The storage ID is 'null' or empty")
        if (cacheType == null)
            throw new NullPointerException("The cache type is 'null'")
        PDResultsMapStorage st = mapStorages.get(storageID)
        if (st == null) {
            st = new PDRamResultsMapStorage<K, V>(storageID)
            mapStorages.put(storageID, st)
        }
        st
    }

    @Override
    synchronized def <T extends Serializable> void deleteMapStorage(String storageID) {
        if (storageID == null || storageID.empty)
            throw new IllegalArgumentException("The storage ID is 'null' or empty")
        mapStorages.remove(storageID)
    }

    @Override
    synchronized String generateUniqueStorageID() {
        def r = new Random()
        return "${System.currentTimeMillis()}_${r.nextInt(1000000)}"
    }
}

/**
 * A RAM implementation of a PD results storage for unordered data collection.
 *
 * @param < T >    The type of data stored.
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDRamResultsCollectionStorage<T extends Serializable> implements PDResultsCollectionStorage<T> {

    final String storageID

    final List<T> dataBuffer = new ArrayList<T>()

    final HashMap<T, T> uniqueDataContained = new HashMap<T, T>()

    PDRamResultsCollectionStorage(String storageID) {
        if (storageID == null)
            throw new NullPointerException("The storage ID is 'null'")
        this.storageID = storageID
    }

    @Override
    void clear() {
        dataBuffer.clear()
        uniqueDataContained.clear()
    }

    @Override
    synchronized void addResults(Collection<T> c) {
        if (c == null)
            throw new NullPointerException("The specified collection is 'null'")
        dataBuffer.addAll(c)
        def it = c.iterator()
        while (it.hasNext()) {
            T item = it.next()
            uniqueDataContained.put(item, item)
        }
    }

    @Override
    synchronized Collection<T> getResults(long startIdx, long endIdx) {
        if (startIdx < 0 || startIdx > dataBuffer.size() - 1)
            throw new IllegalArgumentException("The startIdx is not valid. Buffer size: ${dataBuffer.size()}, startIdx: ${startIdx}")
        if (endIdx <= startIdx || endIdx > dataBuffer.size())
            throw new IllegalArgumentException("The endIdx is not valid. Buffer size: ${dataBuffer.size()}, startIdx: ${startIdx} endIdx: ${endIdx}")
        /*List ret = []
        for (long i = startIdx; i < endIdx; i++)
            ret << dataBuffer.get((int)i)
        ret*/
        dataBuffer.subList(startIdx as Integer, endIdx as Integer)
    }

    @Override
    synchronized long size() {
        return dataBuffer.size()
    }

    @Override
    synchronized boolean contains(T item) {
        if (item == null)
            throw new NullPointerException("The item is 'null'")
        return uniqueDataContained.containsKey(item)
    }
}

/**
 * A RAM implementation of a PD results map storage.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDRamResultsMapStorage<K extends Serializable, V extends Serializable> implements PDResultsMapStorage<K, V> {

    final String storageID
    final Map<K, V> mapValues

    PDRamResultsMapStorage(String storageID) {
        this(storageID, 10000, 0.3f)
    }

    PDRamResultsMapStorage(String storageID, int capacity, float loadFactor) {
        if (storageID == null)
            throw new NullPointerException("The storage ID is 'null'")
        this.storageID = storageID
        mapValues = new HashMap<K, V>(capacity, loadFactor)
    }

    @Override
    synchronized void remove(K k) {
        if (k == null)
            throw new NullPointerException("The specified key is 'null'")
        mapValues.remove("key_" + k)
    }

    @Override
    synchronized V get(K k) {
        if (k == null)
            throw new NullPointerException("The specified key is 'null'")
        V ret = mapValues.get(k)
        ret
    }

    @Override
    synchronized void put(K k, V value) {
        if (k == null)
            throw new NullPointerException("The key value is 'null'")
        if (value == null)
            throw new NullPointerException("The specified value is 'null'")
        mapValues.put(k, value)
    }

    @Override
    Iterator<K> keys() {
        mapValues.keySet().iterator()
    }

    @Override
    synchronized long size() {
        mapValues.size()
    }

    @Override
    synchronized boolean containsKey(K k) {
        mapValues.containsKey("key_" + k)
    }
}

/**
 * A RAM implementation of a PD results storage for ordered data set.
 *
 * @param < T >    The type of data stored.
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDRamResultsSortedSetStorage<K extends Comparable & Serializable, V extends Serializable> implements PDResultsSortedSetStorage<K, V> {

    final String storageID

    final TreeSet<SortedSetItem<K, V>> dataBuffer = new TreeSet<SortedSetItem<K, V>>()
    final boolean sortAscending

    PDRamResultsSortedSetStorage(String storageID, boolean sortAscending) {
        if (storageID == null)
            throw new NullPointerException("The storage ID is 'null'")
        this.storageID = storageID
        this.sortAscending = sortAscending
    }

    @Override
    boolean isSortedAscending() {
        return sortAscending
    }

    @Override
    synchronized void addResults(Collection<SortedSetItem<K, V>> c) {
        if (c == null)
            throw new NullPointerException("The specified collection is 'null'")
        dataBuffer.addAll(c)
    }

    @Override
    synchronized Collection<V> getResults(long startIdx, long endIdx) {
        if (startIdx < 0 || startIdx > dataBuffer.size() - 1)
            throw new IllegalArgumentException("The startIdx is not valid. Buffer size: ${dataBuffer.size()}, startIdx: ${startIdx}")
        if (endIdx <= startIdx || endIdx > dataBuffer.size())
            throw new IllegalArgumentException("The endIdx is not valid. Buffer size: ${dataBuffer.size()}, startIdx: ${startIdx} endIdx: ${endIdx}")
        if (sortAscending)
            getSubSet(dataBuffer.iterator(), startIdx, endIdx)
        else
            getSubSet(dataBuffer.descendingIterator(), startIdx, endIdx)
    }

    private Collection<V> getSubSet(Iterator<SortedSetItem> iter, long fromIndex, long endIndex) {
        long idx = 0
        ArrayList l = new ArrayList()
        while (iter.hasNext()) {
            if (idx < fromIndex) {
                idx++
                iter.next()
                continue
            }
            if (idx >= endIndex)
                break

            l.add(iter.next().item)
            idx++
        }
        l
    }


    @Override
    synchronized long size() {
        return dataBuffer.size()
    }

    @Override
    synchronized boolean contains(SortedSetItem<K, V> item) {
        if (item == null)
            throw new NullPointerException("The item is 'null'")
        return dataBuffer.contains(item)
    }


}