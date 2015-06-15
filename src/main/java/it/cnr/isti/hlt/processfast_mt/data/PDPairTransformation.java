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
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PartitionableDataset#pair(PartitionableDataset)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDPairTransformation<T extends Serializable> implements PDTransformation {
    public PDPairTransformation(MTTaskContext tc, MTPartitionableDataset<T> toMerge, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (toMerge == null) throw new NullPointerException("The partitionable dataset to intersect is 'null'");
        this.tc = tc;
        this.toPair = toMerge;
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
            toPairSize = toPair.count();
        }


        final List toAdd = new ArrayList();
        long curSize = storage.size();
        if (curSize >= toPairSize)
            return;

        List curRes = (List) src.collect(Collectors.toList());
        long maxItemsToAdd = toPairSize - curSize;
        final long itemsToAdd = Math.min(maxItemsToAdd, curRes.size());
        Collection otherDs = toPair.take(curSize, itemsToAdd);
        final Iterator<T> otherDsIterator = ((List<T>) otherDs).iterator();
        for (int idx = 0; idx < curRes.size(); idx++) {
            if (idx >= itemsToAdd)
                break;
            toAdd.add(new Pair(curRes.get(idx), otherDsIterator.next()));
        }

        storage.addResults(toAdd);
    }

    @Override
    public PDResultsCollectionStorageIteratorProvider getFinalResults(Map internalResults) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) internalResults.get("storage");
        internalResults.remove("storage");
        return new PDResultsCollectionStorageIteratorProvider(storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }

    public final MTTaskContext getTc() {
        return tc;
    }

    public final MTPartitionableDataset<T> getToPair() {
        return toPair;
    }

    public final int getMaxBufferSize() {
        return maxBufferSize;
    }

    private final MTTaskContext tc;
    private final MTPartitionableDataset<T> toPair;
    private final int maxBufferSize;
    private long toPairSize;
}
