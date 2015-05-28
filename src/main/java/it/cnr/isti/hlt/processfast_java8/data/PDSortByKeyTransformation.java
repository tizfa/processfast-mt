package it.cnr.isti.hlt.processfast_java8.data;

import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast.data.PairPartitionableDataset;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_java8.core.GParsTaskContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PairPartitionableDataset#sortByKey(boolean)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDSortByKeyTransformation<K extends Comparable & Serializable, V extends Serializable> implements PDTransformation {
    public PDSortByKeyTransformation(GParsTaskContext tc, int maxBufferSize, boolean sortAscending) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");

        this.tc = tc;
        this.maxBufferSize = maxBufferSize;
        this.sortAscending = sortAscending;
    }

    @Override
    public Stream applyTransformation(Stream source) {
        Stream<Pair<K, V>> src = source;
        return src.sorted((o1, o2) -> o1.getV1().compareTo(o2.getV1()));
    }

    @Override
    public boolean needAllAvailableData() {
        return true;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType) {
        PDResultsSortedSetStorage storage = (PDResultsSortedSetStorage) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createSortedSetStorage(storageManager.generateUniqueStorageID(), cacheType, sortAscending);
            dest.put("storage", storage);
        } else {
            storage = (PDResultsSortedSetStorage) dest;
        }


        List c = (List) src.collect(Collectors.toList());
        List toAdd = new ArrayList();
        Iterator<Pair<K, V>> iterator = c.iterator();
        while (iterator.hasNext()) {
            Pair<K, V> item = iterator.next();
            toAdd.add(new SortedSetItem(item.getV1(), item));
        }

        storage.addResults(toAdd);
    }

    @Override
    public PDResultsSortedSetStorageIteratorProvider getFinalResults(Map internalResults) {
        PDResultsSortedSetStorage storage = (PDResultsSortedSetStorage) internalResults.get("storage");
        internalResults.remove("storage");
        return new PDResultsSortedSetStorageIteratorProvider(storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }

    public final GParsTaskContext getTc() {
        return tc;
    }

    public final int getMaxBufferSize() {
        return maxBufferSize;
    }

    public final boolean getSortAscending() {
        return sortAscending;
    }

    public final boolean isSortAscending() {
        return sortAscending;
    }

    private final GParsTaskContext tc;
    private final int maxBufferSize;
    private final boolean sortAscending;
}
