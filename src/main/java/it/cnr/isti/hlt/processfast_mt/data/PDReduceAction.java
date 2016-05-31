/*
 *
 * ****************
 * Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ******************
 */

package it.cnr.isti.hlt.processfast_mt.data;

import groovy.transform.CompileStatic;
import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast.data.ImmutableDataSourceIteratorProvider;
import it.cnr.isti.hlt.processfast.data.PDFunction2;
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;

@CompileStatic
public class PDReduceAction<Out extends Serializable> implements PDAction<Out> {
    public PDReduceAction(MTTaskContext tc, PDFunction2<Out, Out, Out> code) {
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
    public <T extends Serializable> Out computeFinalResultsDirectlyOnDataSourceIteratorProvider(ImmutableDataSourceIteratorProvider<T> provider) {
        return null;
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

    private final MTTaskContext tc;
    private final PDFunction2<Out, Out, Out> code;
}
