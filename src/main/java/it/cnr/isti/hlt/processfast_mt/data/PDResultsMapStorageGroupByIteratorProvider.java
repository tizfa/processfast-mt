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

import it.cnr.isti.hlt.processfast.data.DataIterable;
import it.cnr.isti.hlt.processfast.utils.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * A PD results map storage iterator provider suitable for "groupBy" transformation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDResultsMapStorageGroupByIteratorProvider<K extends Serializable, V extends Serializable> implements PDResultsStorageIteratorProvider<Pair<K, DataIterable<V>>> {
    public PDResultsMapStorageGroupByIteratorProvider(PDResultsMapStorage storage, int maxBufferSize) {
        if (storage == null) throw new NullPointerException("The storage is 'null'");
        this.storage = storage;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Iterator<Pair<K, DataIterable<V>>> iterator() {
        return new PDResultsMapStorageGroupByIterator<K, V>(storage, maxBufferSize);
    }

    @Override
    public boolean sizeEnabled() {
        return true;
    }

    @Override
    public long size() {
        return storage.size();
    }

    @Override
    public boolean contains(Pair<K, DataIterable<V>> item) {
        return storage.containsKey(item.getV1());
    }

    @Override
    public boolean containsEnabled() {
        return true;
    }

    @Override
    public Collection<Pair<K, DataIterable<V>>> take(long startFrom, long numItems) {
        Iterator<Pair<K, DataIterable<V>>> it = iterator();
        long idx = 0;
        ArrayList<Pair<K, DataIterable<V>>> ret = new ArrayList<>();
        while (it.hasNext()) {
            Pair<K, DataIterable<V>> v = it.next();
            if (idx >= startFrom && idx < (startFrom + numItems))
                ret.add(v);
            idx++;
            if (idx >= (startFrom + numItems))
                break;
        }
        return ret;
    }

    @Override
    public boolean takeEnabled() {
        return true;
    }

    public final PDResultsMapStorage getStorage() {
        return storage;
    }

    private final PDResultsMapStorage storage;
    private int maxBufferSize;
}
