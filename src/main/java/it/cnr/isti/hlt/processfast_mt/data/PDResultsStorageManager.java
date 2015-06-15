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

/**
 * A generic storage manager for intermediate results in PDs operations. Every
 * implementation must be thread safe.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public interface PDResultsStorageManager {
    /**
     * Get the storage manager ID.
     *
     * @return The storage manager ID.
     */
    String getStorageManagerID();

    /**
     * Create a storage containing PD results in the form of a unordered data collection. If the storage does not exist, it will
     * be created. If the storage exists, it will be returned.
     *
     * @param storageID The storage ID.
     * @param cacheType Indicate where to store the storage.
     * @return The requested storage ID.
     */
    <T extends Serializable> PDResultsCollectionStorage<T> createCollectionStorage(String storageID, CacheType cacheType);

    /**
     * Delete the collection storage with the given ID.
     *
     * @param storageID The storage ID.
     */
    <T extends Serializable> void deleteCollectionStorage(String storageID);

    /**
     * Create a storage containing PD results in the form of a sorted data set. If the storage does not exist, it will
     * bne created. If the storage exists, it will be returned.
     *
     * @param storageID     The storage ID.
     * @param cacheType     Indicate where to store the storage.
     * @param sortAscending True if the data must be ordered ascending, false if must be ordered descending.
     * @return The requested storage ID.
     */
    <K extends Comparable & Serializable, V extends Serializable> PDResultsSortedSetStorage<K, V> createSortedSetStorage(String storageID, CacheType cacheType, boolean sortAscending);

    /**
     * Delete the sorted storage with the given ID.
     *
     * @param storageID The storage ID.
     */
    <T extends Serializable> void deleteSortedSetStorage(String storageID);

    /**
     * Create a storage containing PD results in the form of unordered dictionary data. If the storage does not exist, it will
     * bne created. If the storage exists, it will be returned.
     *
     * @param storageID The storage ID.
     * @param cacheType Indicate where to store the storage.
     * @return The requested storage ID.
     */
    <K extends Serializable, V extends Serializable> PDResultsMapStorage<K, V> createMapStorage(String storageID, CacheType cacheType);

    /**
     * Delete the map storage with the given ID.
     *
     * @param storageID The storage ID.
     */
    <T extends Serializable> void deleteMapStorage(String storageID);

    /**
     * Generate unique storage ID in the storage manager.
     *
     * @return An unique storage ID.
     */
    String generateUniqueStorageID();
}

