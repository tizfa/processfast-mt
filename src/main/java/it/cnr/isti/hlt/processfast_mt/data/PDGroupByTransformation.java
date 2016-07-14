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
import it.cnr.isti.hlt.processfast.data.PDFunction;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PartitionableDataset#groupBy(PDFunction)}  operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDGroupByTransformation<T extends Serializable, K extends Serializable> implements PDTransformation {
    public PDGroupByTransformation(MTTaskContext tc, PDFunction<T, K> code, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (code == null) throw new NullPointerException("The programmer's code is 'null'");
        this.tc = tc;
        this.code = code;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    @Override
    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Stream applyTransformation(PartitionableDataset pd, Stream source) {
        final GParsTaskDataContext tdc = new GParsTaskDataContext(tc, pd);
        Map grouped = (Map) source.collect(Collectors.groupingBy(item -> code.call(tdc, (T) item)));
        return grouped.entrySet().parallelStream();
    }

    @Override
    public boolean needAllAvailableData() {
        return true;
    }

    @Override
    public void mergeResults(PartitionableDataset pd, PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType) {
        PDResultsMapStorage<K, ArrayList> storage = (PDResultsMapStorage<K, ArrayList>) dest.get("storage");

        if (storage == null) {
            storage = storageManager.createMapStorage(storageManager.generateUniqueStorageID(), cacheType);
            dest.put("storage", storage);
        }

        List<Map.Entry<K, ArrayList>> res = (List) src.collect(Collectors.toList());
        for (Map.Entry<K, ArrayList> item : res) {
            ArrayList listStorage = storage.get(item.getKey());
            if (listStorage == null) {
                listStorage = new ArrayList<T>();
                storage.put(item.getKey(), listStorage);
            }
            listStorage.addAll(item.getValue());
        }

    }

    @Override
    public PDResultsStorageIteratorProvider getFinalResults(PartitionableDataset pd, Map internalResults) {
        PDResultsMapStorage storage = (PDResultsMapStorage) internalResults.get("storage");
        internalResults.remove("storage");
        return new PDResultsMapStorageGroupByIteratorProvider(storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }

    private final PDFunction<T, K> code;
    private final MTTaskContext tc;
    private int maxBufferSize;
}
