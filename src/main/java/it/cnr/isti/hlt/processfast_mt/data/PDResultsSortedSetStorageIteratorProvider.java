package it.cnr.isti.hlt.processfast_mt.data;

import java.io.Serializable;
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

    public final PDResultsSortedSetStorage getStorage() {
        return storage;
    }

    private final PDResultsSortedSetStorage storage;
    private int maxBufferSize;
}
