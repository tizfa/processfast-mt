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

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public interface PDAction<Out> {

    /**
     * **************  Operations performed while processing aa data source that need to be transformed before applying this action.
     */

    /**
     * Apply the action on a source collection.
     *
     * @param source
     * @return
     */
    Out applyAction(Stream source);

    /**
     * Merge the results in "src" to the results in "dest".
     *
     * @param storageManager The storage manager available.
     * @param src            The source results.
     * @param dest           The destination results or 'null' if there are no results in dest.
     * @param cacheType      The type of cache where to merge results. Used when dest is 'null' and a new
     *                       structure for store intermediate results needs to be created.
     * @return The merged results.
     */
    void mergeResults(PDResultsStorageManager storageManager, Out src, Map dest, CacheType cacheType);

    /**
     * Indicate if the action need more results to compute the
     * desired behaviour.
     *
     * @param currentResults The current results.
     * @return True if the action need more results, false otherwise.
     */
    boolean needMoreResults(Map currentResults);

    /**
     * Get the final results to be returned to action caller by
     * translating the specified final internal results.
     *
     * @param internalResults The computed final internal results.
     * @return The final results for action caller.
     */
    Out getFinalResults(PDResultsStorageManager storageManager, Map internalResults);


    /**
     * **************  Operations performed while processing aa data source that NOT need to be transformed before applying this action.
     */

    /**
     * Compute the action results directly using the definitive data source iterator provider. If the action can not be performed directly,
     * 'null' will be returned.
     *
     * @param provider The data source iterator provider to use.
     * @param <T>      The item type in the data source.
     * @return The final results or 'null' if the final results can't be computed directly.
     */
    <T extends Serializable> Out computeFinalResultsDirectlyOnDataSourceIteratorProvider(ImmutableDataSourceIteratorProvider<T> provider);
}

