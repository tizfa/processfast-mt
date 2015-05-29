package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PartitionableDataset#cartesian(PartitionableDataset)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDCartesianTransformation<T extends Serializable> implements PDTransformation {
    public PDCartesianTransformation(MTTaskContext tc, MTPartitionableDataset<T> toIntersect, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (toIntersect == null)
            throw new NullPointerException("The partitionable dataset to use in cartesian product is 'null'");
        this.tc = tc;
        this.toIntersect = toIntersect;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Stream applyTransformation(Stream source) {
        return source;
    }

    @Override
    public boolean needAllAvailableData() {
        return true;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType);
            dest.put("storage", storage);
            //toIntersectCache = (GParsPartitionableDataset) toIntersect.cache(CacheType.ON_DISK)
            toIntersectCache = (MTPartitionableDataset) toIntersect;
            toIntersectSize = toIntersectCache.count();
        }


        List coll = (List) src.collect(Collectors.toList());
        for (Object item : coll) {
            addCartesianItems(storage, item, toIntersectCache, toIntersectSize, maxBufferSize);
        }
    }

    private void addCartesianItems(PDResultsCollectionStorage storage, final Object item, MTPartitionableDataset<T> pd, long pdSize, int maxBufferSize) {
        long curIdx = 0;
        final List l = new ArrayList();
        while (true) {
            long numItems = Math.min(maxBufferSize, pdSize - curIdx);
            if (numItems < 1) break;
            List<T> l2 = pd.take(curIdx, numItems);

            for (Object item2 : l2) {
                l.add(new Pair(item, item2));
            }

            storage.addResults(l);
            l.clear();
            curIdx += numItems;
        }

    }

    @Override
    public PDResultsCollectionStorageIteratorProvider getFinalResults(Map internalResults) {
        toIntersectCache.close();
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) internalResults.get("storage");
        internalResults.remove("storage");
        return new PDResultsCollectionStorageIteratorProvider(storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }

    public final MTTaskContext getTc() {
        return tc;
    }

    public final MTPartitionableDataset<T> getToIntersect() {
        return toIntersect;
    }

    public MTPartitionableDataset<T> getToIntersectCache() {
        return toIntersectCache;
    }

    public void setToIntersectCache(MTPartitionableDataset<T> toIntersectCache) {
        this.toIntersectCache = toIntersectCache;
    }

    public long getToIntersectSize() {
        return toIntersectSize;
    }

    public void setToIntersectSize(long toIntersectSize) {
        this.toIntersectSize = toIntersectSize;
    }

    public final int getMaxBufferSize() {
        return maxBufferSize;
    }

    private final MTTaskContext tc;
    private final MTPartitionableDataset<T> toIntersect;
    private MTPartitionableDataset<T> toIntersectCache;
    private long toIntersectSize = 0;
    private final int maxBufferSize;
}
