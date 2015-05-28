package it.cnr.isti.hlt.processfast_java8.data;


import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast_java8.core.GParsTaskContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PDCollectAction<Out extends Serializable> implements PDAction<Collection<Out>> {
    public PDCollectAction(GParsTaskContext tc) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        this.tc = tc;
    }

    @Override
    public Collection<Out> applyAction(Stream source) {
        List res = (List) source.collect(Collectors.toList());
        return res;
    }


    @Override
    public Collection<Out> getFinalResults(PDResultsStorageManager storageManager, Map internalResults) {
        PDResultsCollectionStorage<Out> results = (PDResultsCollectionStorage<Out>) internalResults.get("storage");
        if (results.size() == 0)
            return new ArrayList();
        Collection<Out> res = results.getResults(0, results.size());
        storageManager.deleteCollectionStorage(results.getStorageID());
        internalResults.remove("storage");
        return res;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Collection<Out> src, Map dest, CacheType cacheType) {
        PDResultsCollectionStorage<Out> storage = (PDResultsCollectionStorage<Out>) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType);
            storage.addResults(src);
            dest.put("storage", storage);
        } else {
            storage.addResults(src);
        }

    }

    @Override
    public boolean needMoreResults(Map currentResults) {
        return true;
    }

    private final GParsTaskContext tc;
}
