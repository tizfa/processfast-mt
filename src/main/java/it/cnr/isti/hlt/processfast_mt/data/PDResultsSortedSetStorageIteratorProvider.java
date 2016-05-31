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
import java.util.Iterator;

/**
 * A PD results iterators provider for sorted set storage.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDResultsSortedSetStorageIteratorProvider<T extends Serializable> implements PDResultsStorageIteratorProvider<T> {
    public PDResultsSortedSetStorageIteratorProvider(PDResultsSortedSetStorage storage, int maxBufferSize) {
        if (storage == null) throw new NullPointerException("The storage is 'null'");
        this.storage = storage;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Iterator<T> iterator() {
        return new PDResultsSortedSetStorageIterator<T>(storage, maxBufferSize);
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
    public boolean contains(T item) {
        return false;
    }

    @Override
    public boolean containsEnabled() {
        return false;
    }

    @Override
    public Collection<T> take(long startFrom, long numItems) {
        return null;
    }

    @Override
    public boolean takeEnabled() {
        return false;
    }

    public final PDResultsSortedSetStorage getStorage() {
        return storage;
    }

    private final PDResultsSortedSetStorage storage;
    private int maxBufferSize;
}
