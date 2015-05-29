package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.utils.Pair;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A PD results map storage iterator provider which returns for each key the first result available in form of
 * a {@link Pair} class.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDResultsMapStoragePairIteratorProvider<K extends Serializable, V extends Serializable> implements PDResultsStorageIteratorProvider<Pair<K, V>> {
    public PDResultsMapStoragePairIteratorProvider(PDResultsMapStorage storage, int maxBufferSize) {
        if (storage == null) throw new NullPointerException("The storage is 'null'");
        this.storage = storage;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Iterator<Pair<K, V>> iterator() {
        return new PDResultsMapStoragePairIterator<K, V>(storage, maxBufferSize);
    }

    public final PDResultsMapStorage<K, V> getStorage() {
        return storage;
    }

    private final PDResultsMapStorage<K, V> storage;
    private int maxBufferSize;
}
