package it.cnr.isti.hlt.processfast_mt.data;

import groovy.transform.CompileStatic;
import it.cnr.isti.hlt.processfast.data.DataIterable;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A PD results list storage adapter implementing a data iterable object.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
public class PDResultsListDataIterable<T extends Serializable> implements DataIterable<T> {
    public PDResultsListDataIterable(PDResultsCollectionStorage storage, int maxBufferSize) {
        if (storage == null) throw new NullPointerException("The specified storage is 'null'");
        this.storage = storage;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Iterator<T> iterator() {
        return new PDResultsCollectionStorageIterator(storage, maxBufferSize);
    }

    public final PDResultsCollectionStorage getStorage() {
        return storage;
    }

    private final PDResultsCollectionStorage storage;
    private int maxBufferSize;
}
