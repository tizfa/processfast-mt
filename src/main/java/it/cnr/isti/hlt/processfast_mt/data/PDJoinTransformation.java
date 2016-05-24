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
import it.cnr.isti.hlt.processfast.data.PairPartitionableDataset;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A PD {@link PairPartitionableDataset#join(PairPartitionableDataset)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDJoinTransformation<K extends Serializable, V extends Serializable, T extends Serializable> implements PDTransformation {
    public PDJoinTransformation(MTTaskContext tc, PairPartitionableDataset<K, T> dataset, int maxBufferSize) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (dataset == null) throw new NullPointerException("The dataset to join is 'null'");
        this.tc = tc;
        this.dataset = dataset;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Stream applyTransformation(Stream source) {
        if (datasetKeys == null) {
            datasetKeys = dataset.map((tdc, v) -> v.getV1()).cache(CacheType.ON_DISK);
        }

        Stream<Pair<K, V>> s = source;
        final GParsTaskDataContext tdc = new GParsTaskDataContext(tc);
        return s.filter(item -> datasetKeys.contains(item.getV1()));
    }

    @Override
    public boolean needAllAvailableData() {
        return true;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType) {
        PDResultsMapStorage storage = (PDResultsMapStorage) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createMapStorage(storageManager.generateUniqueStorageID(), cacheType);
            dest.put("storage", storage);
        }

        Collection<Pair<K, V>> coll = (Collection<Pair<K, V>>) src.collect(Collectors.toList());
        for (Pair<K, V> item : coll) {
            if (storage.containsKey(item.getV1()))
                throw new IllegalArgumentException("A key with value " + item.getV1() + " is already stored on temporary data. Be sure that original data source does not contains key duplicates!");
            storage.put(item.getV1(), item);
        }
    }

    @Override
    public PDResultsMapStoragePairIteratorProvider getFinalResults(Map internalResults) {
        PDResultsMapStorage storage = (PDResultsMapStorage) internalResults.get("storage");
        internalResults.remove("storage");

        // Perform join of the two datasets.
        long toRead = dataset.count();
        long startFrom = 0;
        while (toRead > 0) {
            List<Pair<K, T>> tmp = dataset.take(startFrom, Math.min(maxBufferSize, toRead));
            for (Pair<K, T> it : tmp) {
                if (storage.containsKey(it.getV1())) {
                    // Update its content.
                    Pair<K, V> item = (Pair<K, V>) storage.get(it.getV1());
                    storage.remove(it.getV1());
                    storage.put(it.getV1(), new Pair<V, T>(item.getV2(), it.getV2()));
                }
            }
            startFrom += tmp.size();
            toRead -= tmp.size();
        }


        return new PDResultsMapStoragePairIteratorProvider<K, Pair<V, T>>(storage, maxBufferSize);
    }

    @Override
    public boolean isRealTransformation() {
        return true;
    }


    private final PairPartitionableDataset<K, T> dataset;
    private PartitionableDataset<K> datasetKeys;
    private final MTTaskContext tc;
    private final int maxBufferSize;
}
