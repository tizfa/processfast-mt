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
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PartitionableDataset#union(PartitionableDataset)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
public class PDUnionTransformation<T extends Serializable> implements PDTransformation {
    public PDUnionTransformation(MTTaskContext tc, MTPartitionableDataset<T> toMerge, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (toMerge == null) throw new NullPointerException("The partitionable dataset to intersect is 'null'");
        this.tc = tc;
        this.toMerge = toMerge;
        this.toMerge.activateSystemThreadPool = false;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Stream applyTransformation(Stream source) {
        return source;
    }

    @Override
    public boolean needAllAvailableData() {
        return true;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType);
            dest.put("storage", storage);
        }

        List res = (List) src.collect(Collectors.toList());
        storage.addResults(res);
    }

    @Override
    public PDResultsCollectionStorageIteratorProvider getFinalResults(Map internalResults) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) internalResults.get("storage");
        boolean done = false;
        long s = toMerge.count();
        long startIdx = 0;
        while (!done) {
            long toRetrieve = Math.min(maxBufferSize, s - startIdx);
            if (toRetrieve == 0) break;
            List<T> items = toMerge.take(startIdx, toRetrieve);
            storage.addResults(items);
            startIdx += items.size();
        }

        internalResults.remove("storage");
        this.toMerge.activateSystemThreadPool = true;

        return new PDResultsCollectionStorageIteratorProvider(storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }

    public final MTTaskContext getTc() {
        return tc;
    }

    public final MTPartitionableDataset<T> getToMerge() {
        return toMerge;
    }

    public final int getMaxBufferSize() {
        return maxBufferSize;
    }

    @Override
    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    private final MTTaskContext tc;
    private final MTPartitionableDataset<T> toMerge;
    private int maxBufferSize;
}
