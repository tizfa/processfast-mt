package it.cnr.isti.hlt.processfast_mt.data;

import groovy.transform.CompileStatic;
import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast.data.PDFunction2;
import it.cnr.isti.hlt.processfast_mt.core.GParsTaskContext;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;

@CompileStatic
public class PDReduceAction<Out extends Serializable> implements PDAction<Out> {
    public PDReduceAction(GParsTaskContext tc, PDFunction2<Out, Out, Out> code) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (code == null) throw new NullPointerException("The programmer's code is ' null'");
        this.tc = tc;
        this.code = code;
    }

    @Override
    public Out applyAction(Stream source) {
        final GParsTaskDataContext tdc = new GParsTaskDataContext(tc);

        return (Out) source.reduce((i1, i2) -> {
            return code.call(tdc, (Out) i1, (Out) i2);
        }).get();
    }

    @Override
    public Out getFinalResults(PDResultsStorageManager storageManager, Map internalResults) {
        return (Out) internalResults.get("res");
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Out src, Map dest, CacheType cacheType) {
        Out cur = (Out) dest.get("res");
        if (cur == null)
            dest.put("res", src);
        else
            dest.put("res", code.call(new GParsTaskDataContext(tc), cur, src));
    }

    @Override
    public boolean needMoreResults(Map currentResults) {
        return true;
    }

    private final GParsTaskContext tc;
    private final PDFunction2<Out, Out, Out> code;
}
