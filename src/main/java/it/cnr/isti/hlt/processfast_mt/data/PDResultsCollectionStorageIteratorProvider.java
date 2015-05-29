package it.cnr.isti.hlt.processfast_mt.data;


import java.io.Serializable;
import java.util.Iterator;

/**
 * A PD results iterators provider for collection storage.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDResultsCollectionStorageIteratorProvider<T extends Serializable> implements PDResultsStorageIteratorProvider<T> {
    public PDResultsCollectionStorageIteratorProvider(PDResultsCollectionStorage storage, int maxBufferSize) {
        if (storage == null) throw new NullPointerException("The storage is 'null'");
        this.storage = storage;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Iterator<T> iterator() {
        return new PDResultsCollectionStorageIterator<T>(storage, maxBufferSize);
    }

    public final PDResultsCollectionStorage getStorage() {
        return storage;
    }

    private final PDResultsCollectionStorage storage;
    private int maxBufferSize;
}

