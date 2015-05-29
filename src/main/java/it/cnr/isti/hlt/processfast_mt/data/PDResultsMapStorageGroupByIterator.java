package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.CollectionDataIterable;
import it.cnr.isti.hlt.processfast.data.DataIterable;
import it.cnr.isti.hlt.processfast.utils.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A PD results map storage iterator suitable fro "groupBy" transformation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDResultsMapStorageGroupByIterator<K extends Serializable, V extends Serializable> implements Iterator<Pair<K, DataIterable<V>>> {
    public PDResultsMapStorageGroupByIterator(PDResultsMapStorage storage, int maxBufferSize) {
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
    public Pair<K, DataIterable<V>> next() {
        if (!hasNext()) throw new NoSuchElementException("No more items in storage");
        K key = (K) keys.next();
        Collection<V> listStorage = storage.get(key);
        Pair p = new Pair(key, new CollectionDataIterable<V>(listStorage));
        return null;
    }

    private PDResultsMapStorage<K, ArrayList<V>> storage;
    private Iterator keys;
    private int maxBufferSize;
}
