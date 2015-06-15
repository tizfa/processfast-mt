/*
 *
 * ****************
 * Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
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
 * ******************
 */

package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.CacheType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * A RAM implementation of a PD results storage manager.
 * <p>
 * Every created storage (indipendently if requested to be created in
 * RAM or on disk) will be created on RAM.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDRamResultsStorageManager implements PDResultsStorageManager {
    public PDRamResultsStorageManager(String storageManagerID) {
        if (storageManagerID == null || storageManagerID.isEmpty())
            throw new IllegalArgumentException("The storage manager ID is 'null' or empty");
        this.storageManagerID = storageManagerID;
    }

    @Override
    public String getStorageManagerID() {
        return this.storageManagerID;
    }

    @Override
    public synchronized <T extends Serializable> PDResultsCollectionStorage<T> createCollectionStorage(String storageID, CacheType cacheType) {
        if (storageID == null || storageID.isEmpty())
            throw new IllegalArgumentException("The storage ID is 'null' or empty");
        if (cacheType == null) throw new NullPointerException("The cache type is 'null'");
        PDResultsCollectionStorage st = collectionStorages.get(storageID);
        if (st == null) {
            st = new PDRamResultsCollectionStorage<T>(storageID);
            collectionStorages.put(storageID, st);
        }

        return st;
    }

    @Override
    public synchronized <T extends Serializable> void deleteCollectionStorage(String storageID) {
        if (storageID == null || storageID.isEmpty())
            throw new IllegalArgumentException("The storage ID is 'null' or empty");
        collectionStorages.remove(storageID);
    }

    @Override
    public synchronized <K extends Comparable & Serializable, V extends Serializable> PDResultsSortedSetStorage<K, V> createSortedSetStorage(String storageID, CacheType cacheType, boolean sortAscending) {
        if (storageID == null || storageID.isEmpty())
            throw new IllegalArgumentException("The storage ID is 'null' or empty");
        if (cacheType == null) throw new NullPointerException("The cache type is 'null'");
        PDResultsSortedSetStorage st = sortedSetStorages.get(storageID);
        if (st == null) {
            st = new PDRamResultsSortedSetStorage<K, V>(storageID, sortAscending);
            sortedSetStorages.put(storageID, (PDRamResultsSortedSetStorage) st);
        }

        return st;
    }

    @Override
    public synchronized <T extends Serializable> void deleteSortedSetStorage(String storageID) {
        if (storageID == null || storageID.isEmpty())
            throw new IllegalArgumentException("The storage ID is 'null' or empty");
        sortedSetStorages.remove(storageID);
    }

    @Override
    public synchronized <K extends Serializable, V extends Serializable> PDResultsMapStorage<K, V> createMapStorage(String storageID, CacheType cacheType) {
        if (storageID == null || storageID.isEmpty())
            throw new IllegalArgumentException("The storage ID is 'null' or empty");
        if (cacheType == null) throw new NullPointerException("The cache type is 'null'");
        PDResultsMapStorage st = mapStorages.get(storageID);
        if (st == null) {
            st = new PDRamResultsMapStorage<K, V>(storageID);
            mapStorages.put(storageID, (PDRamResultsMapStorage) st);
        }

        return st;
    }

    @Override
    public synchronized <T extends Serializable> void deleteMapStorage(String storageID) {
        if (storageID == null || storageID.isEmpty())
            throw new IllegalArgumentException("The storage ID is 'null' or empty");
        mapStorages.remove(storageID);
    }

    @Override
    public synchronized String generateUniqueStorageID() {
        final Random r = new Random();
        return String.valueOf(System.currentTimeMillis()) + "_" + String.valueOf(r.nextInt(1000000));
    }


    /**
     * The set of managed collection storages.
     */
    private final Map<String, PDResultsCollectionStorage> collectionStorages = new HashMap<String, PDResultsCollectionStorage>();
    /**
     * The set of managed map storages.
     */
    private final Map<String, PDRamResultsMapStorage> mapStorages = new HashMap<String, PDRamResultsMapStorage>();
    /**
     * The set of managed sorted set storages.
     */
    private final Map<String, PDRamResultsSortedSetStorage> sortedSetStorages = new HashMap<String, PDRamResultsSortedSetStorage>();
    private String storageManagerID;
}

