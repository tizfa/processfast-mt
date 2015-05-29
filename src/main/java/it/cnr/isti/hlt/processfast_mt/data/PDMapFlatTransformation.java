package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast.data.PDFunction;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PartitionableDataset#mapFlat(PDFunction)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDMapFlatTransformation<T, Out> implements PDTransformation {

    public PDMapFlatTransformation(MTTaskContext tc, PDFunction<T, Iterator<Out>> code, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (code == null) throw new NullPointerException("The programmer's code is 'null'");
        this.tc = tc;
        this.code = code;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Stream applyTransformation(Stream src) {
        final GParsTaskDataContext tdc = new GParsTaskDataContext(tc);
        Stream<T> source = src;
        return source.flatMap(it -> {
            Iterator<Out> res = code.call(tdc, it);
            ArrayList ret = new ArrayList();
            while (res.hasNext())
                ret.add(res.next());
            return ret.parallelStream();
        });
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

        List ret = (List) src.collect(Collectors.toList());
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

    private final PDFunction<T, Iterator<Out>> code;
    private final MTTaskContext tc;
    private final int maxBufferSize;
}
