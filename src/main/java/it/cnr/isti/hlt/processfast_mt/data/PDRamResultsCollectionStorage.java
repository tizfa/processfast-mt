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
import java.util.*;

/**
 * A RAM implementation of a PD results storage for unordered data collection.
 *
 * @param < T >    The type of data stored.
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDRamResultsCollectionStorage<T extends Serializable> implements PDResultsCollectionStorage<T> {
    public PDRamResultsCollectionStorage(String storageID) {
        if (storageID == null) throw new NullPointerException("The storage ID is 'null'");
        this.storageID = storageID;
    }

    @Override
    public void clear() {
        dataBuffer.clear();
        uniqueDataContained.clear();
    }

    @Override
    public synchronized void addResults(Collection<T> c) {
        if (c == null) throw new NullPointerException("The specified collection is 'null'");
        dataBuffer.addAll(c);
        Iterator<T> it = c.iterator();
        while (it.hasNext()) {
            T item = it.next();
            uniqueDataContained.put(item, item);
        }

    }

    @Override
    public synchronized Collection<T> getResults(final long startIdx, final long endIdx) {
        if (startIdx < 0 || startIdx > dataBuffer.size() - 1)
            throw new IllegalArgumentException("The startIdx is not valid. Buffer size: " + String.valueOf(dataBuffer.size()) + ", startIdx: " + String.valueOf(startIdx));
        if (endIdx <= startIdx || endIdx > dataBuffer.size())
            throw new IllegalArgumentException("The endIdx is not valid. Buffer size: " + String.valueOf(dataBuffer.size()) + ", startIdx: " + String.valueOf(startIdx) + " endIdx: " + String.valueOf(endIdx));
        return dataBuffer.subList((int) startIdx, (int) endIdx);
    }

    @Override
    public synchronized long size() {
        return dataBuffer.size();
    }

    @Override
    public synchronized boolean contains(T item) {
        if (item == null) throw new NullPointerException("The item is 'null'");
        return uniqueDataContained.containsKey(item);
    }

    public final String getStorageID() {
        return storageID;
    }


    public final HashMap<T, T> getUniqueDataContained() {
        return uniqueDataContained;
    }

    private final String storageID;
    private final List<T> dataBuffer = new ArrayList<T>();
    private final HashMap<T, T> uniqueDataContained = new HashMap<T, T>();
}
