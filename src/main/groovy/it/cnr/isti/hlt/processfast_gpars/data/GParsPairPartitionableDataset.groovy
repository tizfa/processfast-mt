/*
 * *****************
 *  Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
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
 * *******************
 */

package it.cnr.isti.hlt.processfast_gpars.data

import groovy.transform.CompileStatic
import it.cnr.isti.hlt.processfast.core.TaskDataContext
import it.cnr.isti.hlt.processfast.data.*
import it.cnr.isti.hlt.processfast.utils.Pair
import it.cnr.isti.hlt.processfast.utils.Procedure3
import it.cnr.isti.hlt.processfast_gpars.core.GParsTaskContext

/**
 * An implementation of a pair partitionable dataset based on GPars.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class GParsPairPartitionableDataset<K extends Serializable, V extends Serializable> extends GParsPartitionableDataset<Pair<K, V>> implements PairPartitionableDataset<K, V> {

    GParsPairPartitionableDataset(GParsTaskContext tc, ImmutableDataSourceIteratorProvider<Pair<K, V>> provider) {
        super(tc, provider)
    }


    GParsPairPartitionableDataset(GParsPartitionableDataset<Pair<K, V>> previousPD) {
        super(previousPD)
    }

    @Override
    def PairPartitionableDataset<K, V> reduceByKey(PDFunction2<V, V, V> func) {
        def pd = new GParsPairPartitionableDataset<K, V>(this)
        pd.transformations.add(new PDReduceByKeyTransformation<K, V>(tc, func, maxPartitionSize))
        pd
    }

    @Override
    PairPartitionableDataset<K, V> sortByKey(boolean ascending) {
        def pd = new GParsPairPartitionableDataset<K, V>(this)
        pd.transformations.add(new PDSortByKeyTransformation<K, V>(tc, maxPartitionSize, ascending))
        pd
    }

    @Override
    PairPartitionableDataset<K, DataIterable<V>> groupByKey() {
        return null
    }

    @Override
    def <T extends Serializable> PairPartitionableDataset<K, Pair<V, T>> join(PairPartitionableDataset<K, T> dataset) {
        return null
    }

    @Override
    PairPartitionableDataset<K, V> enableLocalComputation(boolean enable) {
        // Ignored on a multi-thread runtime. Always local computation!
        return new GParsPairPartitionableDataset<K, V>(this)
    }

    @Override
    PairPartitionableDataset<K, V> cache(CacheType cacheType) {
        GParsPartitionableDataset cached = (GParsPartitionableDataset) super.cache(cacheType)
        return new GParsPairPartitionableDataset<K, V>(cached.tc, cached.dataSourceIteratorProvider)
    }

    @Override
    PairPartitionableDataset<K, V> saveOnStorageManager(Procedure3<TaskDataContext, StorageManager, Pair<K, V>> func) {
        super.saveOnStorageManager(func)
        this
    }
}
