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
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;


public class PDContainsAction<ItemType extends Serializable> implements PDAction<Boolean> {

    public PDContainsAction(MTTaskContext tc, ItemType item) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        if (item == null) throw new NullPointerException("The item is 'null'");
        this.tc = tc;
        this.item = item;
    }

    @Override
    public Boolean applyAction(PartitionableDataset pd, Stream source) {
        return source.anyMatch(i -> i.equals(item));
    }

    @Override
    public Boolean getFinalResults(PartitionableDataset pd, PDResultsStorageManager storageManager, Map internalResults) {
        return (boolean) internalResults.get("res");
    }

    @Override
    public <T extends Serializable> Boolean computeFinalResultsDirectlyOnDataSourceIteratorProvider(PartitionableDataset pd, ImmutableDataSourceIteratorProvider<T> provider) {
        if (!provider.containsEnabled())
            return null;
        if (itemContained == null)
            itemContained = provider.contains((T) item);
        return itemContained;
    }

    @Override
    public void mergeResults(PartitionableDataset pd, PDResultsStorageManager storageManager, Boolean src, Map dest, CacheType cacheType) {
        Boolean cur = (Boolean) dest.get("res");
        if (cur == null)
            dest.put("res", src);
        else {
            dest.put("res", src || cur);
        }
    }

    @Override
    public boolean needMoreResults(PartitionableDataset pd, Map currentResults) {
        boolean containsItem = (boolean) currentResults.get("res");
        return !containsItem;
    }

    private Boolean itemContained;
    private final MTTaskContext tc;
    private final ItemType item;
}
