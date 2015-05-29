package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.CacheType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * A Gpars PD action for {@link GParsPartitionableDataset#count()} method.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDCountAction implements PDAction<Long> {
    @Override
    public Long applyAction(Stream source) {
        return source.count();
    }

    @Override
    public Long getFinalResults(PDResultsStorageManager storageManager, Map internalResults) {
        return (long) internalResults.get("res");
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Long src, Map dest, CacheType cacheType) {
        Long cur = (Long) dest.get("res");
        if (cur == null)
            dest.put("res", src);
        else
            dest.put("res", src + cur);

    }

    @Override
    public boolean needMoreResults(Map currentResults) {
        return true;
    }

}
