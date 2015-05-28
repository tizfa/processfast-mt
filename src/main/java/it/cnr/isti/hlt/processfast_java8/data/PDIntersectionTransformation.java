package it.cnr.isti.hlt.processfast_java8.data;

import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast_java8.core.GParsTaskContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PartitionableDataset#intersection(PartitionableDataset)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDIntersectionTransformation<T extends Serializable> implements PDTransformation {

    public PDIntersectionTransformation(GParsTaskContext tc, GParsPartitionableDataset<T> toIntersect, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (toIntersect == null) throw new NullPointerException("The partitionable dataset to intersect is 'null'");
        this.tc = tc;
        this.toIntersect = toIntersect;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Stream applyTransformation(Stream source) {
        return source.filter(item -> toIntersect.contains((T) item)).distinct();
    }

    @Override
    public boolean needAllAvailableData() {
        return true;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType) {
        PDResultsCollectionStorage<T> storage = (PDResultsCollectionStorage<T>) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType);
            dest.put("storage", storage);
        }

        final ArrayList toAdd = new ArrayList();
        List<T> res = (List) src.collect(Collectors.toList());
        for (T item : res) {
            if (!storage.contains(item))
                toAdd.add(item);
        }

        storage.addResults(toAdd);
    }

    @Override
    public PDResultsCollectionStorageIteratorProvider getFinalResults(Map internalResults) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) internalResults.get("storage");
        internalResults.remove("storage");
        return new PDResultsCollectionStorageIteratorProvider((PDResultsCollectionStorage<T>) storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }

    private final GParsTaskContext tc;
    private final GParsPartitionableDataset<T> toIntersect;
    private final int maxBufferSize;
}
