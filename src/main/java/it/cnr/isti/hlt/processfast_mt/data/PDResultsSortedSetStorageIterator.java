package it.cnr.isti.hlt.processfast_mt.data;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A PD results iterator for sorted set storage.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDResultsSortedSetStorageIterator<T extends Serializable> implements Iterator<T> {
    public PDResultsSortedSetStorageIterator(PDResultsSortedSetStorage storage, int maxBufferSize) {
        if (storage == null) throw new NullPointerException("The storage is 'null'");
        this.storage = storage;
        this.maxBufferSize = maxBufferSize;

        // Read first set of data.
        long minSize = Math.min(storage.size(), maxBufferSize);
        if (minSize != 0) {
            buffer = new ArrayList<>();
            buffer.addAll(storage.getResults(0, minSize));
        } else
            buffer = new ArrayList();
        storageSize = storage.size();
    }

    public PDResultsSortedSetStorageIterator(PDResultsSortedSetStorage storage) {
        this(storage, 100000);
    }

    @Override
    public boolean hasNext() {
        return (readIdx + curIdx) < storageSize;
    }

    @Override
    public T next() {
        if (!hasNext()) return null;
        if (curIdx >= buffer.size()) {
            readIdx += buffer.size();
            buffer = new ArrayList<>();
            buffer.addAll(storage.getResults(readIdx, Math.min(storageSize, readIdx + maxBufferSize)));
            curIdx = 0;
        }


        Object val = buffer.get(curIdx);
        curIdx = curIdx++;
        return (T) val;
    }

    private int curIdx = 0;
    private PDResultsSortedSetStorage storage;
    private List buffer;
    private int maxBufferSize;
    private long readIdx = 0;
    private long storageSize = 0;
}
