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

import java.util.Map;
import java.util.stream.Stream;

/**
 * Record handle for a real partitionable dataset transformation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public interface PDTransformation extends PDBaseTransformation {

    int getMaxBufferSize();

    void setMaxBufferSize(int maxBufferSize);

    /**
     * Apply a specific transformation on source collection.
     *
     * @param pd     The parent partitionable dataset.
     * @param source The source data collection.
     * @return The resulting collection after the operation has been applied.
     */
    Stream applyTransformation(PartitionableDataset pd, Stream source);

    /**
     * Indicate if the transformation need all available data or just some data chunk
     * to perform correctly.
     *
     * @return True if alla the data is necessary, false otherwise.
     */
    boolean needAllAvailableData();

    /**
     * Merge the results in "src" to the results in "dest".
     *
     * @param pd             The parent partitionable dataset.
     * @param storageManager The storage manager to use.
     * @param src            The source results.
     * @param dest           The destination results or 'null' if there are no results in dest.
     * @param cacheType      The type of cache where to merge results. Used when dest is 'null' and a new
     *                       structure for store intermediate results needs to be created.
     * @return The merged results.
     */
    void mergeResults(PartitionableDataset pd, PDResultsStorageManager storageManager, Stream src, Map dest, CacheType cacheType);

    /**
     * Get the final results to be returned to action caller by
     * translating the specified final internal results.
     *
     * @param pd              The parent partitionable dataset.
     * @param internalResults The computed final internal results.
     * @return The final results for action caller.
     */
    PDResultsStorageIteratorProvider getFinalResults(PartitionableDataset pd, Map internalResults);
}

