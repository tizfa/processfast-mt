package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast_mt.core.GParsTaskContext;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;


public class PDContainsAction<ItemType extends Serializable> implements PDAction<Boolean> {

    public PDContainsAction(GParsTaskContext tc, ItemType item) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (item == null) throw new NullPointerException("The item is 'null'");
        this.tc = tc;
        this.item = item;
    }

    @Override
    public Boolean applyAction(Stream source) {
        return source.anyMatch(i -> i.equals(item));
    }

    @Override
    public Boolean getFinalResults(PDResultsStorageManager storageManager, Map internalResults) {
        return (boolean) internalResults.get("res");
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Boolean src, Map dest, CacheType cacheType) {
        Boolean cur = (Boolean) dest.get("res");
        if (cur == null)
            dest.put("res", src);
        else {
            dest.put("res", src || cur);
        }
    }

    @Override
    public boolean needMoreResults(Map currentResults) {
        boolean containsItem = (boolean) currentResults.get("res");
        return !containsItem;
    }


    private final GParsTaskContext tc;
    private final ItemType item;
}
