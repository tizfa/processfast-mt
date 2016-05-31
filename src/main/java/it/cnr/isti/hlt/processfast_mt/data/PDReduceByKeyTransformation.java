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

import it.cnr.isti.hlt.processfast.data.CacheType;
import it.cnr.isti.hlt.processfast.data.PDFunction2;
import it.cnr.isti.hlt.processfast.data.PairPartitionableDataset;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PairPartitionableDataset#reduceByKey(PDFunction2)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDReduceByKeyTransformation<K extends Serializable, V extends Serializable> implements PDTransformation {
    public PDReduceByKeyTransformation(MTTaskContext tc, PDFunction2<V, V, V> code, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (code == null) throw new NullPointerException("The programmer's code is 'null");

        this.tc = tc;
        this.code = code;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Stream applyTransformation(Stream src) {
        GParsTaskDataContext tdc = new GParsTaskDataContext(tc);

        Stream<Pair<K, V>> source = src;
        Map<K, V> res = (Map) source.collect(Collectors.groupingBy((Pair<K, V> item) -> item.getV1(), new PDFunctionCollector(tdc, code)));
        final ArrayList<Pair<K, V>> values = new ArrayList<Pair<K, V>>(res.size());
        res.entrySet().stream().forEach(node->{
            values.add(new Pair<>(node.getKey(), node.getValue()));
        });
        return values.parallelStream();
    }

    @Override
    public boolean needAllAvailableData() {
        return true;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType) {
        PDResultsMapStorage<K, V> storage = (PDResultsMapStorage<K, V>) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createMapStorage(storageManager.generateUniqueStorageID(), cacheType);
            dest.put("storage", storage);
        }

        final PDResultsMapStorage<K, V> st = storage;
        final GParsTaskDataContext tdc = new GParsTaskDataContext(tc);
        Collection<Pair<K, V>> c = (Collection) src.collect(Collectors.toList());
        c.stream().forEach(item -> {
            V curVal = item.getV2();
            V storedValue = st.get(item.getV1());
            if (storedValue != null) {
                curVal = code.call(tdc, storedValue, curVal);
            }

            st.put(item.getV1(), curVal);
        });

    }

    @Override
    public PDResultsStorageIteratorProvider getFinalResults(Map internalResults) {
        PDResultsMapStorage storage = (PDResultsMapStorage) internalResults.get("storage");
        internalResults.remove("storage");
        return new PDResultsMapStoragePairIteratorProvider(storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }

    public final MTTaskContext getTc() {
        return tc;
    }

    public final int getMaxBufferSize() {
        return maxBufferSize;
    }

    @Override
    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    public final PDFunction2<V, V, V> getCode() {
        return code;
    }

    private final MTTaskContext tc;
    private int maxBufferSize;
    private final PDFunction2<V, V, V> code;
}
