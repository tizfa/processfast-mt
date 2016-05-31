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
import it.cnr.isti.hlt.processfast_mt.core.MTTaskContext;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PDCollectAction<Out extends Serializable> implements PDAction<Collection<Out>> {
    public PDCollectAction(MTTaskContext tc) {
        if (tc == null) throw new NullPointerException("The task context is 'null'");
        this.tc = tc;
    }

    @Override
    public Collection<Out> applyAction(Stream source) {
        List res = (List) source.collect(Collectors.toList());
        return res;
    }


    @Override
    public Collection<Out> getFinalResults(PDResultsStorageManager storageManager, Map internalResults) {
        PDResultsCollectionStorage<Out> results = (PDResultsCollectionStorage<Out>) internalResults.get("storage");
        if (results.size() == 0)
            return new ArrayList();
        Collection<Out> res = results.getResults(0, results.size());
        storageManager.deleteCollectionStorage(results.getStorageID());
        internalResults.remove("storage");
        ArrayList ret = new ArrayList();
        ret.addAll(res);
        return ret;
    }

    @Override
    public <T extends Serializable> Collection<Out> computeFinalResultsDirectlyOnDataSourceIteratorProvider(ImmutableDataSourceIteratorProvider<T> provider) {
        if (outColl == null) {
            outColl = new ArrayList<>();
            Iterator<T> it = provider.iterator();
            while (it.hasNext()) {
                outColl.add((Out) it.next());
            }
        }
        return outColl;
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Collection<Out> src, Map dest, CacheType cacheType) {
        PDResultsCollectionStorage<Out> storage = (PDResultsCollectionStorage<Out>) dest.get("storage");
        if (storage == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType);
            storage.addResults(src);
            dest.put("storage", storage);
        } else {
            storage.addResults(src);
        }

    }

    @Override
    public boolean needMoreResults(Map currentResults) {
        return true;
    }

    private List<Out> outColl;

    private final MTTaskContext tc;
}
