package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PDTakeAction<Out extends Serializable> implements PDAction<Collection<Out>> {
    public PDTakeAction(MTTaskContext tc, final long startFrom, final long numItems) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (startFrom < 0)
            throw new IllegalArgumentException("The startFrom parameter value is invalid: " + String.valueOf(startFrom));
        if (numItems < 1)
            throw new IllegalArgumentException("The numItems parameter value is invalid: " + String.valueOf(numItems));

        this.tc = tc;
        this.startFrom = startFrom;
        this.numItems = numItems;
    }

    @Override
    public Collection<Out> applyAction(Stream source) {
        List res = (List) source.skip(startFrom).limit(numItems).collect(Collectors.toList());
        return res;
    }

    @Override
    public Collection<Out> getFinalResults(PDResultsStorageManager storageManager, Map internalResults) {

        PDResultsCollectionStorage<Out> results = (PDResultsCollectionStorage<Out>) internalResults.get("storage");
        final long s = results.size();
        if (s == 0) return new ArrayList();
        if (startFrom >= s)
            throw new IllegalArgumentException("The requested startFrom value is greater than available results size: startFrom [" + String.valueOf(startFrom) + "] >= results size " + String.valueOf(s));
        long endIdx = startFrom + numItems;
        if (endIdx > s)
            throw new IllegalArgumentException("The requested numItems value is greater than available results size: numItems [" + String.valueOf(numItems) + "] >= available items " + String.valueOf((int) s - startFrom));

        Collection<Out> res = results.getResults(startFrom, startFrom + numItems);
        storageManager.deleteCollectionStorage(results.getStorageID());
        internalResults.remove("storage");
        return res;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Collection<Out> src, Map dest, CacheType cacheType) {

        PDResultsCollectionStorage<Out> storage = (PDResultsCollectionStorage<Out>) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType);
            dest.put("storage", storage);
        }
        storage.addResults(src);
    }

    @Override
    public boolean needMoreResults(Map currentResults) {
        return true;
    }


    private final MTTaskContext tc;
    private final long startFrom;
    private final long numItems;
}
