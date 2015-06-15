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

import it.cnr.isti.hlt.processfast.utils.Pair;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A PD results map storage iterator which returns for each key the first result available in form of
 * a {@link Pair} class.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDResultsMapStoragePairIterator<K extends Serializable, V extends Serializable> implements Iterator<Pair<K, V>> {
    public PDResultsMapStoragePairIterator(PDResultsMapStorage storage, int maxBufferSize) {
        if (storage == null) throw new NullPointerException("The storage is 'null'");
        this.storage = storage;

        // Read first set of data.
        keys = storage.keys();
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public boolean hasNext() {
        return keys.hasNext();
    }

    @Override
    public Pair<K, V> next() {
        if (!hasNext()) throw new NoSuchElementException("No more items in storage");
        K key = (K) keys.next();
        Serializable value = storage.get(key);
        Pair p = new Pair(key, value);
        return p;
    }

    private PDResultsMapStorage storage;
    private Iterator keys;
    private int maxBufferSize;
}
