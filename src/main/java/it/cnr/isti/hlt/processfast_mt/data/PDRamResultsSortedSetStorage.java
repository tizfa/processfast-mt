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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * A RAM implementation of a PD results storage for ordered data set.
 *
 * @param < T >    The type of data stored.
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDRamResultsSortedSetStorage<K extends Comparable & Serializable, V extends Serializable> implements PDResultsSortedSetStorage<K, V> {
    public PDRamResultsSortedSetStorage(String storageID, boolean sortAscending) {
        if (storageID == null) throw new NullPointerException("The storage ID is 'null'");
        this.storageID = storageID;
        this.sortAscending = sortAscending;
    }

    @Override
    public boolean isSortedAscending() {
        return sortAscending;
    }

    @Override
    public synchronized void addResults(Collection<SortedSetItem<K, V>> c) {
        if (c == null) throw new NullPointerException("The specified collection is 'null'");
        dataBuffer.addAll(c);
    }

    @Override
    public synchronized Collection<V> getResults(final long startIdx, final long endIdx) {
        if (startIdx < 0 || startIdx > dataBuffer.size() - 1)
            throw new IllegalArgumentException("The startIdx is not valid. Buffer size: " + String.valueOf(dataBuffer.size()) + ", startIdx: " + String.valueOf(startIdx));
        if (endIdx <= startIdx || endIdx > dataBuffer.size())
            throw new IllegalArgumentException("The endIdx is not valid. Buffer size: " + String.valueOf(dataBuffer.size()) + ", startIdx: " + String.valueOf(startIdx) + " endIdx: " + String.valueOf(endIdx));
        if (sortAscending) return getSubSet(dataBuffer.iterator(), startIdx, endIdx);
        else return getSubSet(dataBuffer.descendingIterator(), startIdx, endIdx);
    }

    private Collection<V> getSubSet(Iterator<SortedSetItem<K, V>> iter, long fromIndex, long endIndex) {
        long idx = 0;
        ArrayList l = new ArrayList();
        while (iter.hasNext()) {
            if (idx < fromIndex) {
                idx = idx++;
                iter.next();
                continue;
            }

            if (idx >= endIndex) break;

            l.add(iter.next().getItem());
            idx = idx++;
        }

        return l;
    }

    @Override
    public synchronized long size() {
        return dataBuffer.size();
    }

    @Override
    public synchronized boolean contains(SortedSetItem<K, V> item) {
        if (item == null) throw new NullPointerException("The item is 'null'");
        return dataBuffer.contains(item);
    }

    public final String getStorageID() {
        return storageID;
    }

    public final TreeSet<SortedSetItem<K, V>> getDataBuffer() {
        return dataBuffer;
    }

    public final boolean getSortAscending() {
        return sortAscending;
    }

    public final boolean isSortAscending() {
        return sortAscending;
    }

    private final String storageID;
    private final TreeSet<SortedSetItem<K, V>> dataBuffer = new TreeSet<SortedSetItem<K, V>>();
    private final boolean sortAscending;
}
