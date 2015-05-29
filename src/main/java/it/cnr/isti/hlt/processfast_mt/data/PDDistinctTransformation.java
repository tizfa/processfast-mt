package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast_mt.core.GParsTaskContext;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PartitionableDataset#distinct()} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDDistinctTransformation<T extends Serializable> implements PDTransformation {

    public PDDistinctTransformation(GParsTaskContext tc, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        this.tc = tc;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Stream applyTransformation(Stream source) {
        return source.distinct();
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

        Collection<T> ret = (Collection<T>) src.collect(Collectors.toList());
        storage.addResults(ret);
    }

    @Override
    public PDResultsCollectionStorageIteratorProvider getFinalResults(Map internalResults) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) internalResults.get("storage");
        internalResults.remove("storage");
        return new PDResultsCollectionStorageIteratorProvider(storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }

    private final GParsTaskContext tc;
    private final int maxBufferSize;
}
