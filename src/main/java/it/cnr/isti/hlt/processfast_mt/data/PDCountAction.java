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

import java.util.Map;
import java.util.stream.Stream;

/**
 * A Gpars PD action for {@link MTPartitionableDataset#count()} method.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDCountAction implements PDAction<Long> {
    @Override
    public Long applyAction(Stream source) {
        return source.count();
    }

    @Override
    public Long getFinalResults(PDResultsStorageManager storageManager, Map internalResults) {
        return (long) internalResults.get("res");
    }

    @Override
    public void mergeResults(PDResultsStorageManager storageManager, Long src, Map dest, CacheType cacheType) {
        Long cur = (Long) dest.get("res");
        if (cur == null)
            dest.put("res", src);
        else
            dest.put("res", src + cur);

    }

    @Override
    public boolean needMoreResults(Map currentResults) {
        return true;
    }

}
