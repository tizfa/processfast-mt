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
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PartitionableDataset#intersection(PartitionableDataset)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDIntersectionTransformation<T extends Serializable> implements PDTransformation {

    public PDIntersectionTransformation(MTTaskContext tc, MTPartitionableDataset<T> toIntersect, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (toIntersect == null) throw new NullPointerException("The partitionable dataset to intersect is 'null'");
        this.tc = tc;
        this.toIntersect = toIntersect;
        this.toIntersect.activateSystemThreadPool = false;
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
        return source.filter(item -> toIntersect.contains((T) item)).distinct();
    }

    @Override
    public boolean needAllAvailableData() {
        return true;
    }

    @Override
    public void mergeResults(PartitionableDataset pd, PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType) {
        PDResultsCollectionStorage<T> storage = (PDResultsCollectionStorage<T>) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType);
            dest.put("storage", storage);
        }

        final ArrayList toAdd = new ArrayList();
        List<T> res = (List) src.collect(Collectors.toList());
        for (T item : res) {
            if (!storage.contains(item))
                toAdd.add(item);
        }

        storage.addResults(toAdd);
    }

    @Override
    public PDResultsCollectionStorageIteratorProvider getFinalResults(PartitionableDataset pd, Map internalResults) {
        this.toIntersect.activateSystemThreadPool = true;
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) internalResults.get("storage");
        internalResults.remove("storage");
        return new PDResultsCollectionStorageIteratorProvider((PDResultsCollectionStorage<T>) storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }

    private final MTTaskContext tc;
    private final MTPartitionableDataset<T> toIntersect;
    private int maxBufferSize;
}
