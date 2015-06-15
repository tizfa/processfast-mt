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
import java.util.Iterator;

/**
 * A storage containing PD temporary results in forms of a dictionary. Every implementation must be
 * thread-safe.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public interface PDResultsMapStorage<Key extends Serializable, V extends Serializable> extends Serializable {
    /**
     * Get the storage ID.
     *
     * @return The storage ID.
     */
    String getStorageID();

    /**
     * Remove the item with specified key.
     *
     * @param key The key of the item to be removed.
     */
    void remove(Key key);

    /**
     * Get the value associated with the specified key.
     *
     * @param key The key to search.
     * @return The requested value or 'null' if the value can not be found.
     */
    V get(Key key);

    /**
     * Associate to specified key the given value.
     *
     * @param key   The key value.
     * @param value The value associated.
     */
    void put(Key key, V value);

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
    long size();

    /**
     * Check if the specified item is contained in this
     * storage.
     *
     * @param key
     * @return True if the item is contained in this storage, false otherwise.
     */
    boolean containsKey(Key key);
}
