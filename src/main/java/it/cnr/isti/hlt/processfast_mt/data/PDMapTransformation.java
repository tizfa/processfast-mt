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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PartitionableDataset#map(PDFunction)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDMapTransformation<T, Out> implements PDTransformation {

    public PDMapTransformation(MTTaskContext tc, PDFunction<T, Out> code, int maxBufferSize) {
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
        return source.map(item -> code.call(tdc, (T) item));
    }

    @Override
    public boolean needAllAvailableData() {
        return false;
    }

    @Override
    public void mergeResults(PartitionableDataset pd, PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType);
            dest.put("storage", storage);
        }


        storage.addResults((List) src.collect(Collectors.toList()));
    }

    @Override
    public PDResultsCollectionStorageIteratorProvider getFinalResults(PartitionableDataset pd, Map internalResults) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) internalResults.get("storage");
        internalResults.remove("storage");
        return new PDResultsCollectionStorageIteratorProvider(storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }

    private final PDFunction<T, Out> code;
    private final MTTaskContext tc;
    private int maxBufferSize;
}
