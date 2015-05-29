package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast.data.PDFunction;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast_mt.core.GParsTaskContext;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PartitionableDataset#filter(PDFunction)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDFilterTransformation<T> implements PDTransformation {
    public PDFilterTransformation(GParsTaskContext tc, PDFunction<T, Boolean> code, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (code == null) throw new NullPointerException("The programmer's code is 'null'");
        this.tc = tc;
        this.code = code;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Stream applyTransformation(Stream source) {
        final GParsTaskDataContext tdc = new GParsTaskDataContext(tc);
        return source.filter(item -> code.call(tdc, (T) item));
    }

    @Override
    public boolean needAllAvailableData() {
        return false;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType);
            dest.put("storage", storage);
        }

        storage.addResults((Collection<T>) src.collect(Collectors.toList()));
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


    private final PDFunction<T, Boolean> code;
    private final GParsTaskContext tc;
    private final int maxBufferSize;
}