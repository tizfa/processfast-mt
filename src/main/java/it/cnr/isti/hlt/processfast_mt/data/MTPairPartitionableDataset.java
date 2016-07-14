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

import it.cnr.isti.hlt.processfast.core.TaskDataContext;
import it.cnr.isti.hlt.processfast.data.*;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast.utils.Procedure3;
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;

/**
 * An implementation of a pair partitionable dataset based on GPars.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class MTPairPartitionableDataset<K extends Serializable, V extends Serializable> extends MTPartitionableDataset<Pair<K, V>> implements PairPartitionableDataset<K, V> {

    public MTPairPartitionableDataset(MTTaskContext tc, ImmutableDataSourceIteratorProvider<Pair<K, V>> provider) {
        super(tc, provider);
    }


    public MTPairPartitionableDataset(MTPartitionableDataset previousPD) {
        super(previousPD);
    }

    @Override
    public PairPartitionableDataset<K, V> reduceByKey(PDFunction2<V, V, V> func) {
        MTPairPartitionableDataset pd = new MTPairPartitionableDataset<K, V>(this);
        pd.transformations.add(new PDReduceByKeyTransformation<K, V>(this.tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public PairPartitionableDataset<K, V> sortByKey(boolean ascending) {
        MTPairPartitionableDataset<K, V> pd = new MTPairPartitionableDataset<>(this);
        pd.transformations.add(new PDSortByKeyTransformation(this.tc, maxPartitionSize, ascending));
        return pd;
    }

    @Override
    public PairPartitionableDataset<K, DataIterable<V>> groupByKey() {
        MTPairPartitionableDataset<K, DataIterable<V>> pd = new MTPairPartitionableDataset<>(this);
        pd.transformations.add(new PDGroupByKeyTransformation<K, V>(tc, maxPartitionSize));
        return pd;
    }

    @Override
    public <T extends Serializable> PairPartitionableDataset<K, Pair<V, T>> join(PairPartitionableDataset<K, T> dataset) {
        MTPairPartitionableDataset<K, Pair<V, T>> pd = new MTPairPartitionableDataset<>(this);
        pd.transformations.add(new PDJoinTransformation<K, V, T>(tc, dataset, maxPartitionSize));
        return pd;
    }

    @Override
    public PairPartitionableDataset<K, V> enableLocalComputation(boolean enable) {
        // Ignored on a multi-thread runtime. Always local computation!
        return new MTPairPartitionableDataset<K, V>(this);
    }

    @Override
    public PartitionableDataset<V> values() {
        return map((ctx, v) -> {
            return v.getV2();
        });
    }

    @Override
    public <T extends Serializable> PairPartitionableDataset<K, T> mapValues(PDFunction<V, T> func) {
        return mapPair((ctx, v) -> {
            T ret = func.call(ctx, v.getV2());
            return new Pair<K, T>(v.getV1(), ret);
        });
    }

    @Override
    public PairPartitionableDataset<K, V> cache(CacheType cacheType) {
        MTPartitionableDataset cached = (MTPartitionableDataset) super.cache(cacheType);
        MTPairPartitionableDataset<K, V> ret = new MTPairPartitionableDataset<K, V>(cached.getTc(), cached.dataSourceIteratorProvider);
        ret.maxPartitionSize = cached.maxPartitionSize;
        ret.activateSystemThreadPool = cached.activateSystemThreadPool;
        return ret;
    }

    @Override
    public PairPartitionableDataset<K, V> saveOnStorageManager(Procedure3<TaskDataContext, StorageManager, Pair<K, V>> func) {
        super.saveOnStorageManager(func);
        return this;
    }

    @Override
    public PairPartitionableDataset<K, V> distinct() {
        MTPairPartitionableDataset<K, V> pd = new MTPairPartitionableDataset<K, V>(this);
        pd.transformations.add(new PDDistinctTransformation<Pair<K, V>>(tc, maxPartitionSize));
        return pd;
    }

    @Override
    public PairPartitionableDataset<K, V> withPartitionSize(int partitionSize) {
        if (partitionSize < 1)
            throw new IllegalArgumentException("The partition size is invalid: ${partitionSize}");

        MTPairPartitionableDataset<K, V> pd = new MTPairPartitionableDataset<K, V>(this);
        PDCustomizeTransformation ct = new PDCustomizeTransformation();
        ct.setCustomizationCode((MTPartitionableDataset pad) -> {
            pad.maxPartitionSize = partitionSize;
        });
        pd.transformations.add(ct);
        return pd;
    }

    @Override
    public PairPartitionableDataset<K, V> withInputData(String key, Serializable value) {
        if (key == null || key.isEmpty())
            throw new IllegalArgumentException("The key value is 'null' or empty");
        if (value == null)
            throw new IllegalArgumentException("The input value is 'null'");

        MTPairPartitionableDataset<K, V> pd = new MTPairPartitionableDataset<K, V>(this);
        PDCustomizeTransformation ct = new PDCustomizeTransformation();
        ct.setCustomizationCode((MTPartitionableDataset pad) -> {
            pad.inputValues.put(key, value);
        });
        pd.transformations.add(ct);
        return pd;
    }

    @Override
    public PairPartitionableDataset<K, V> union(PartitionableDataset<Pair<K, V>> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'");
        if (!(dataset instanceof MTPartitionableDataset))
            throw new IllegalArgumentException("The dataset to intersect must be of type ${GParsPartitionableDataset.class.name}");

        MTPairPartitionableDataset<K, V> pd = new MTPairPartitionableDataset<K, V>(this);
        pd.transformations.add(new PDUnionTransformation<Pair<K, V>>(tc, (MTPartitionableDataset<Pair<K, V>>) dataset, maxPartitionSize));
        return pd;
    }
}
