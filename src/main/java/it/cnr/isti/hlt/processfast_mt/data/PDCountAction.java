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
import it.cnr.isti.hlt.processfast.data.ImmutableDataSourceIteratorProvider;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A Gpars PD action for {@link MTPartitionableDataset#count()} method.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDCountAction implements PDAction<Long> {

    private long computedCount = -1;

    @Override
    public Long applyAction(PartitionableDataset pd, Stream source) {
        return source.count();
    }

    @Override
    public Long getFinalResults(PartitionableDataset pd, PDResultsStorageManager storageManager, Map internalResults) {
        return (long) internalResults.get("res");
    }

    @Override
    public <T extends Serializable> Long computeFinalResultsDirectlyOnDataSourceIteratorProvider(PartitionableDataset pd, ImmutableDataSourceIteratorProvider<T> provider) {
        if (!provider.sizeEnabled())
            return null;
        if (computedCount == -1)
            computedCount = provider.size();
        return computedCount;
    }

    @Override
    public void mergeResults(PartitionableDataset pd, PDResultsStorageManager storageManager, Long src, Map dest, CacheType cacheType) {
        Long cur = (Long) dest.get("res");
        if (cur == null)
            dest.put("res", src);
        else
            dest.put("res", src + cur);

    }

    @Override
    public boolean needMoreResults(PartitionableDataset pd, Map currentResults) {
        return true;
    }

}
