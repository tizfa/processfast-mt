package it.cnr.isti.hlt.processfast_java8.data;

import groovy.transform.CompileStatic;
import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast.data.PDProcedure;
import it.cnr.isti.hlt.processfast_java8.core.GParsTaskContext;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;

@CompileStatic
public class PDProcessAction<Out extends Serializable> implements PDAction<Void> {
    public PDProcessAction(GParsTaskContext tc, PDProcedure<Out> func) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (func == null) throw new NullPointerException("The func is 'null'");
        this.tc = tc;
        this.func = func;
    }

    @Override
    public Void applyAction(Stream source) {
        final GParsTaskDataContext tdc = new GParsTaskDataContext(tc);
        source.forEach(item -> {
            func.call(tdc, (Out) item);
        });

        return null;
    }

    @Override
    public Void getFinalResults(PDResultsStorageManager storageManager, Map internalResults) {
        return null;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Void src, Map dest, CacheType cacheType) {
    }

    @Override
    public boolean needMoreResults(Map currentResults) {
        return true;
    }


    private final GParsTaskContext tc;
    private final PDProcedure<Out> func;
}
