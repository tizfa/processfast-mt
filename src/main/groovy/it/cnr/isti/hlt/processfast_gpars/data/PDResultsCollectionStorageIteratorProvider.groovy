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
import it.cnr.isti.hlt.processfast.data.CollectionDataIterable
import it.cnr.isti.hlt.processfast.data.DataIterable
import it.cnr.isti.hlt.processfast.data.ImmutableDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.utils.Pair

/**
 * A PD results storage iterator.
 *
 * @param < T >  The type of data contained in the storage.
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
interface PDResultsStorageIteratorProvider<T extends Serializable> extends ImmutableDataSourceIteratorProvider<T> {
    /**
     * Get the linked storage.
     *
     * @return The linked storage.
     */
    Object getStorage()
}

/**
 * A PD results iterators provider for collection storage.
 *
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDResultsCollectionStorageIteratorProvider<T extends Serializable> implements PDResultsStorageIteratorProvider<T> {

    final PDResultsCollectionStorage storage
    private int maxBufferSize

    PDResultsCollectionStorageIteratorProvider(PDResultsCollectionStorage storage, int maxBufferSize) {
        if (storage == null)
            throw new NullPointerException("The storage is 'null'")
        this.storage = storage
        this.maxBufferSize = maxBufferSize
    }

    @Override
    Iterator<T> iterator() {
        return new PDResultsCollectionStorageIterator<T>(storage, maxBufferSize)
    }
}

/**
 * A PD results iterator for collection storage.
 *
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDResultsCollectionStorageIterator<T extends Serializable> implements Iterator<T> {

    private int curIdx = 0
    private PDResultsCollectionStorage storage
    private List buffer
    private int maxBufferSize
    private long readIdx = 0
    private long storageSize = 0

    PDResultsCollectionStorageIterator(PDResultsCollectionStorage storage, int maxBufferSize = 100000) {
        if (storage == null)
            throw new NullPointerException("The storage is 'null'")
        this.storage = storage
        this.maxBufferSize = maxBufferSize

        // Read first set of data.
        long minSize = Math.min(storage.size(), maxBufferSize)
        if (minSize != 0)
            buffer = storage.getResults(0, minSize).toList()
        else
            buffer = []
        storageSize = storage.size()
    }

    @Override
    boolean hasNext() {
        if ((readIdx + curIdx) >= storageSize)
            return false
        else
            return true
    }

    @Override
    T next() {
        if (!hasNext())
            return null
        if (curIdx >= buffer.size()) {
            readIdx += buffer.size()
            buffer = storage.getResults(readIdx, Math.min(storageSize, readIdx + maxBufferSize)).toList()
            curIdx = 0
        }

        def val = buffer.get(curIdx)
        curIdx++
        return (T) val
    }
}

/**
 * A PD results map storage iterator provider suitable for "groupBy" transformation.
 *
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDResultsMapStorageGroupByIteratorProvider<K extends Serializable, V extends Serializable> implements PDResultsStorageIteratorProvider<Pair<K, DataIterable<V>>> {

    final PDResultsMapStorage storage
    private int maxBufferSize

    PDResultsMapStorageGroupByIteratorProvider(PDResultsMapStorage storage, int maxBufferSize) {
        if (storage == null)
            throw new NullPointerException("The storage is 'null'")
        this.storage = storage
        this.maxBufferSize = maxBufferSize
    }

    @Override
    Iterator<Pair<K, DataIterable<V>>> iterator() {
        return new PDResultsMapStorageGroupByIterator<K, V>(storage, maxBufferSize)
    }
}

/**
 * A PD results map storage iterator suitable fro "groupBy" transformation.
 *
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDResultsMapStorageGroupByIterator<K extends Serializable, V extends Serializable> implements Iterator<Pair<K, DataIterable<V>>> {

    private PDResultsMapStorage<K, ArrayList<V>> storage
    private Iterator keys
    private int maxBufferSize

    PDResultsMapStorageGroupByIterator(PDResultsMapStorage storage, int maxBufferSize) {
        if (storage == null)
            throw new NullPointerException("The storage is 'null'")
        this.storage = storage

        // Read first set of data.
        keys = storage.keys()
        this.maxBufferSize = maxBufferSize
    }

    @Override
    boolean hasNext() {
        keys.hasNext()
    }

    @Override
    Pair<K, DataIterable<V>> next() {
        if (!hasNext())
            throw new NoSuchElementException("No more items in storage")
        K key = (K) keys.next()
        Collection<V> listStorage = (Collection<V>) storage.get(key)
        def p = new Pair(key, new CollectionDataIterable<V>(listStorage))
    }
}

/**
 * A PD results map storage iterator provider which returns for each key the first result available in form of
 * a {@link Pair} class.
 *
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDResultsMapStoragePairIteratorProvider<K extends Serializable, V extends Serializable> implements PDResultsStorageIteratorProvider<Pair<K, V>> {

    final PDResultsMapStorage<K, V> storage
    private int maxBufferSize

    PDResultsMapStoragePairIteratorProvider(PDResultsMapStorage storage, int maxBufferSize) {
        if (storage == null)
            throw new NullPointerException("The storage is 'null'")
        this.storage = storage
        this.maxBufferSize = maxBufferSize
    }

    @Override
    Iterator<Pair<K, V>> iterator() {
        return new PDResultsMapStoragePairIterator<K, V>(storage, maxBufferSize)
    }
}

/**
 * A PD results map storage iterator which returns for each key the first result available in form of
 * a {@link Pair} class.
 *
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDResultsMapStoragePairIterator<K extends Serializable, V extends Serializable> implements Iterator<Pair<K, V>> {

    private PDResultsMapStorage storage
    private Iterator keys
    private int maxBufferSize

    PDResultsMapStoragePairIterator(PDResultsMapStorage storage, int maxBufferSize) {
        if (storage == null)
            throw new NullPointerException("The storage is 'null'")
        this.storage = storage

        // Read first set of data.
        keys = storage.keys()
        this.maxBufferSize = maxBufferSize
    }

    @Override
    boolean hasNext() {
        keys.hasNext()
    }

    @Override
    Pair<K, V> next() {
        if (!hasNext())
            throw new NoSuchElementException("No more items in storage")
        K key = (K) keys.next()
        def value = storage.get(key)
        def p = new Pair(key, value)
    }
}

/**
 * A PD results list storage adapter implementing a data iterable object.
 *
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDResultsListDataIterable<T extends Serializable> implements DataIterable<T> {

    final PDResultsCollectionStorage storage
    private int maxBufferSize

    PDResultsListDataIterable(PDResultsCollectionStorage storage, int maxBufferSize) {
        if (storage == null)
            throw new NullPointerException("The specified storage is 'null'")
        this.storage = storage
        this.maxBufferSize = maxBufferSize
    }

    @Override
    Iterator<T> iterator() {
        return new PDResultsCollectionStorageIterator(storage, maxBufferSize)
    }
}

/**
 * A PD results iterators provider for sorted set storage.
 *
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDResultsSortedSetStorageIteratorProvider<T extends Serializable> implements PDResultsStorageIteratorProvider<T> {

    final PDResultsSortedSetStorage storage
    private int maxBufferSize

    PDResultsSortedSetStorageIteratorProvider(PDResultsSortedSetStorage storage, int maxBufferSize) {
        if (storage == null)
            throw new NullPointerException("The storage is 'null'")
        this.storage = storage
        this.maxBufferSize = maxBufferSize
    }

    @Override
    Iterator<T> iterator() {
        return new PDResultsSortedSetStorageIterator<T>(storage, maxBufferSize)
    }
}

/**
 * A PD results iterator for sorted set storage.
 *
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDResultsSortedSetStorageIterator<T extends Serializable> implements Iterator<T> {

    private int curIdx = 0
    private PDResultsSortedSetStorage storage
    private List buffer
    private int maxBufferSize
    private long readIdx = 0
    private long storageSize = 0

    PDResultsSortedSetStorageIterator(PDResultsSortedSetStorage storage, int maxBufferSize = 100000) {
        if (storage == null)
            throw new NullPointerException("The storage is 'null'")
        this.storage = storage
        this.maxBufferSize = maxBufferSize

        // Read first set of data.
        long minSize = Math.min(storage.size(), maxBufferSize)
        if (minSize != 0)
            buffer = storage.getResults(0, minSize).toList()
        else
            buffer = []
        storageSize = storage.size()
    }

    @Override
    boolean hasNext() {
        if ((readIdx + curIdx) >= storageSize)
            return false
        else
            return true
    }

    @Override
    T next() {
        if (!hasNext())
            return null
        if (curIdx >= buffer.size()) {
            readIdx += buffer.size()
            buffer = storage.getResults(readIdx, Math.min(storageSize, readIdx + maxBufferSize)).toList()
            curIdx = 0
        }

        def val = buffer.get(curIdx)
        curIdx++
        return (T) val
    }
}
