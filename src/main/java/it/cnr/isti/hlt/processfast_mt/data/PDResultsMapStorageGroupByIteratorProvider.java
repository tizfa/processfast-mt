package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.DataIterable;
import it.cnr.isti.hlt.processfast.utils.Pair;

import java.io.Serializable;
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

    public final PDResultsMapStorage getStorage() {
        return storage;
    }

    private final PDResultsMapStorage storage;
    private int maxBufferSize;
}
