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

import java.io.Serializable;
import java.util.Collection;

/**
 * A storage containing PD temporary results in form of a collection of data. Every implementation must be
 * thread-safe.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public interface PDResultsCollectionStorage<T extends Serializable> extends Serializable {
    /**
     * Remove all stored results.
     */
     void clear();

    /**
     * Get the storage ID.
     *
     * @return The storage ID.
     */
    String getStorageID();

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
     * @param endIdx   The end idx (excluded).
     * @return The requested set of results.
     */
    Collection<T> getResults(long startIdx, long endIdx);

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
    boolean contains(T item);
}
