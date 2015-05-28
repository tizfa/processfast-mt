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

package it.cnr.isti.hlt.processfast_gpars.data;

import groovy.transform.CompileStatic
import groovyx.gpars.GParsPool
import groovyx.gpars.pa.AbstractPAWrapper
import it.cnr.isti.hlt.processfast.core.TaskDataContext
import it.cnr.isti.hlt.processfast.data.*
import it.cnr.isti.hlt.processfast.utils.Pair
import it.cnr.isti.hlt.processfast.utils.Procedure3
import it.cnr.isti.hlt.processfast_gpars.core.GParsTaskContext

/**
 * An implementation of a partitionable dataset based on GPars.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class GParsPartitionableDataset<T extends Serializable> implements PartitionableDataset<T> {

    /**
     * The GPars runtime.
     */
    GParsTaskContext tc

    /**
     * The initial data source iterator.
     */
    ImmutableDataSourceIteratorProvider<T> dataSourceIteratorProvider

    /**
     * The maximum size of a partition (max number of items to process in memory). Default is 1000000.
     */
    protected int maxPartitionSize = 1000000

    /**
     * The set of transformations to apply.
     */
    protected List<PDBaseTransformation> transformations

    /**
     * Current storage manager.
     */
    //protected PDResultsStorageManager storageManager


    GParsPartitionableDataset(GParsTaskContext tc, ImmutableDataSourceIteratorProvider<T> provider) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (provider == null)
            throw new NullPointerException("The data source iterator is 'null'")
        this.tc = tc
        this.dataSourceIteratorProvider = provider
        this.transformations = []
    }


    GParsPartitionableDataset(GParsPartitionableDataset<T> previousPD) {
        if (previousPD == null)
            throw new NullPointerException("The previous partitionable dataset is 'null'")
        this.transformations = []
        this.transformations.addAll(previousPD.transformations)
        this.tc = previousPD.tc
        this.dataSourceIteratorProvider = previousPD.dataSourceIteratorProvider
        this.maxPartitionSize = previousPD.maxPartitionSize
    }


    @Override
    PartitionableDataset<T> enableLocalComputation(boolean enable) {
        // Ignored on a multi-thread runtime. Always local computation!
        this
    }

    @Override
    PartitionableDataset<T> withPartitionSize(int partitionSize) {
        if (partitionSize < 1)
            throw new IllegalArgumentException("The partition size is invalid: ${partitionSize}")

        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this)
        PDCustomizeTransformation ct = new PDCustomizeTransformation()
        ct.customizationCode = { GParsPartitionableDataset pad ->
            pad.maxPartitionSize = partitionSize
        }
        pd.transformations.add(ct)
        pd
    }


    protected ImmutableDataSourceIteratorProvider computeAllIntermediateResults(PDResultsStorageManager storageManager, ImmutableDataSourceIteratorProvider<T> provider,
                                                                                List<PDBaseTransformation> transformations,
                                                                                List providerToDelete, List transformationsSplits, CacheType cacheType, boolean computeLast) {
        List<PDBaseTransformation> currentTransformations = []
        transformations.each { tra ->
            if (tra.isRealTransformation()) {
                PDTransformation tr = (PDTransformation) tra
                if (tr.needAllAvailableData()) {
                    currentTransformations.add(tr)
                    transformationsSplits.add(currentTransformations)
                    currentTransformations = []
                } else {
                    currentTransformations.add(tr)
                }
            } else {
                // A customization.
                currentTransformations.add(tra)
                transformationsSplits.add(currentTransformations)
                currentTransformations = []
            }
        }
        if (currentTransformations.size() > 0)
            transformationsSplits.add(currentTransformations)
        else {
            transformationsSplits.add([])
        }

        int numSplits = transformationsSplits.size() - 1
        if (computeLast)
            numSplits = transformationsSplits.size()

        // First generate in sequence each necessary intermediate results.
        ImmutableDataSourceIteratorProvider currentProvider = provider
        for (int i = 0; i < numSplits; i++) {
            List<PDBaseTransformation> curTr = (List<PDBaseTransformation>) transformationsSplits[i]
            if (curTr.size() == 0)
                continue
            PDBaseTransformation last = curTr.get(curTr.size() - 1)
            if (last.isRealTransformation()) {
                List<PDTransformation> toProcess = []
                curTr.each { toProcess.add((PDTransformation) it) }
                currentProvider = computeIntermediateResults(storageManager, currentProvider, toProcess, cacheType)
                providerToDelete.add(currentProvider)
                /*if (providerToDelete.size() > 1) {
                    PDResultsStorageIteratorProvider stProvider
                    def st = providerToDelete.get(0).storage
                    if (st instanceof PDResultsCollectionStorage)
                        storageManager.deleteCollectionStorage(st.storageID)
                    else if (st instanceof PDResultsMapStorage)
                        storageManager.deleteMapStorage(st.storageID)
                    else
                        storageManager.deleteSortedSetStorage(st.storageID)
                    providerToDelete.remove(0)
                }*/
            } else { // It is a customization.
                def res = curTr.subList(0, curTr.size() - 1)
                if (res.size() > 0) {
                    List<PDTransformation> toProcess = []
                    res.each { toProcess.add((PDTransformation) it) }
                    // Compute current results.
                    currentProvider = computeIntermediateResults(storageManager, currentProvider, toProcess, CacheType.ON_DISK)

                    providerToDelete.add(currentProvider)
                    /*if (providerToDelete.size() > 1) {
                        def st = providerToDelete.get(0).storage
                        if (st instanceof PDResultsCollectionStorage)
                            storageManager.deleteCollectionStorage(st.storageID)
                        else if (st instanceof PDResultsMapStorage)
                            storageManager.deleteMapStorage(st.storageID)
                        else
                            storageManager.deleteSortedSetStorage(st.storageID)
                        providerToDelete.remove(0)
                    }*/
                }

                // Customize partitionable dataset state.
                PDCustomizeTransformation custTr = (PDCustomizeTransformation) last
                custTr.customizationCode.call(this)
            }
        }

        currentProvider
    }


    @Override
    PartitionableDataset<T> cache(CacheType cacheType) {
        if (cacheType == null)
            throw new NullPointerException("The cache type is 'null'")
        if (transformations.size() != 0) {
            List transformationsSplits = []
            List providerToDelete = []
            def storageManager = tc.runtime.pdResultsStorageManagerProvider.createStorageManager(tc.runtime.pdResultsStorageManagerProvider.generateUniqueStorageManagerID())
            def computedProvider = computeAllIntermediateResults(storageManager, dataSourceIteratorProvider, transformations, providerToDelete, transformationsSplits, cacheType, true)
            return new GParsPartitionableDataset<T>(tc, computedProvider)
        } else {
            return new GParsPartitionableDataset<T>(tc, dataSourceIteratorProvider)
        }
    }

    @Override
    def <Out extends Serializable> PartitionableDataset<Out> map(PDFunction<T, Out> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'")

        GParsPartitionableDataset<Out> pd = new GParsPartitionableDataset<Out>(this)
        pd.transformations.add(new PDMapTransformation<T, Out>(tc, func, maxPartitionSize))
        pd
    }

    @Override
    def <K extends Serializable, V extends Serializable> PairPartitionableDataset<K, V> mapPair(PDPairFunction<T, K, V> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'")

        GParsPairPartitionableDataset<K, V> pd = new GParsPairPartitionableDataset<K, V>(this)
        pd.transformations.add(new PDMapPairTransformation<T, K, V>(tc, func, maxPartitionSize))
        pd
    }

    @Override
    PartitionableDataset<T> filter(PDFunction<T, Boolean> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'")

        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this)
        pd.transformations.add(new PDFilterTransformation<T>(tc, func, maxPartitionSize))
        pd
    }

    @Override
    def <Out extends Serializable> PartitionableDataset<Out> mapFlat(PDFunction<T, Iterator<Out>> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'")

        GParsPartitionableDataset<Out> pd = new GParsPartitionableDataset<Out>(this)
        pd.transformations.add(new PDMapFlatTransformation<T>(tc, func, maxPartitionSize))
        pd
    }

    @Override
    def <K extends Serializable, V extends Serializable> PairPartitionableDataset<K, V> mapPairFlat(PDFunction<T, Iterator<Pair<K, V>>> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'")

        GParsPairPartitionableDataset<K, V> pd = new GParsPairPartitionableDataset<K, V>(this)
        pd.transformations.add(new PDMapPairFlatTransformation<T>(tc, func, maxPartitionSize))
        return pd
    }


    @Override
    PartitionableDataset<T> union(PartitionableDataset<T> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'")
        if (!(dataset instanceof GParsPartitionableDataset<T>))
            throw new IllegalArgumentException("The dataset to intersect must be of type ${GParsPartitionableDataset.class.name}")

        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this)
        pd.transformations.add(new PDUnionTransformation<T>(tc, (GParsPartitionableDataset<T>) dataset, maxPartitionSize))
        pd
    }

    @Override
    def <T1 extends Serializable> PartitionableDataset<Pair<T, T1>> pair(PartitionableDataset<T1> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'")
        if (!(dataset instanceof GParsPartitionableDataset<T>))
            throw new IllegalArgumentException("The dataset to intersect must be of type ${GParsPartitionableDataset.class.name}")
        GParsPartitionableDataset<Pair<T, T1>> pd = new GParsPartitionableDataset<Pair<T, T1>>(this)
        pd.transformations.add(new PDPairTransformation<T>(tc, (GParsPartitionableDataset) dataset, maxPartitionSize))
        pd
    }

    @Override
    PartitionableDataset<T> intersection(PartitionableDataset<T> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'")
        if (!(dataset instanceof GParsPartitionableDataset<T>))
            throw new IllegalArgumentException("The dataset to intersect must be of type ${GParsPartitionableDataset.class.name}")

        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this)
        pd.transformations.add(new PDIntersectionTransformation<T>(tc, (GParsPartitionableDataset<T>) dataset, maxPartitionSize))
        pd
    }

    @Override
    PartitionableDataset<T> distinct() {
        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this)
        pd.transformations.add(new PDDistinctTransformation<T>(tc, maxPartitionSize))
        pd
    }

    @Override
    PartitionableDataset<T> sort(boolean sortAscending) {
        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this)
        pd.transformations.add(new PDSortTransformation<T>(tc, maxPartitionSize, sortAscending))
        pd
    }

    @Override
    def <K extends Serializable> PairPartitionableDataset<K, DataIterable<T>> groupBy(PDFunction<T, K> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'")
        def pd = new GParsPairPartitionableDataset<K, DataIterable<T>>(this)
        pd.transformations.add(new PDGroupByTransformation<T, K>(tc, func, maxPartitionSize))
        pd
    }

    @Override
    def <U extends Serializable> PairPartitionableDataset<T, U> cartesian(PartitionableDataset<U> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'")
        if (!(dataset instanceof GParsPartitionableDataset<T>))
            throw new IllegalArgumentException("The dataset to use must be of type ${GParsPartitionableDataset.class.name}")

        def pd = new GParsPairPartitionableDataset<T, U>(this)
        pd.transformations.add(new PDCartesianTransformation<T>(tc, (GParsPartitionableDataset) dataset, maxPartitionSize))
        pd
    }

    @Override
    T reduce(PDFunction2<T, T, T> func) {
        if (func == null)
            throw new NullPointerException("The programmer's code is 'null'")
        def action = new PDReduceAction<T>(tc, func)
        (T) computeResults(dataSourceIteratorProvider, transformations, action)
    }


    @Override
    List<T> collect() {
        def action = new PDCollectAction<T>(tc)
        Collection results = (Collection) computeResults(dataSourceIteratorProvider, transformations, action)
        results.toList()
    }

    @Override
    long count() {
        def action = new PDCountAction()
        return (long) computeResults(dataSourceIteratorProvider, transformations, action)
    }

    @Override
    boolean contains(T item) {
        def action = new PDContainsAction<T>(tc, item)
        return computeResults(dataSourceIteratorProvider, transformations, action)
    }

    @Override
    List<T> take(long startFrom, long numItems) {
        if (startFrom < 0)
            throw new IllegalArgumentException("The startFrom value is invalid: ${startFrom}")
        if (numItems < 1)
            throw new IllegalArgumentException("The numItems value is invalid: ${numItems}")

        def action = new PDTakeAction<T>(tc, startFrom, numItems)
        return (List<T>) computeResults(dataSourceIteratorProvider, transformations, action)
    }

    @Override
    PartitionableDataset<T> saveOnStorageManager(Procedure3<TaskDataContext, StorageManager, T> func) {
        // TODO: Add implementation.
        throw new RuntimeException("Not implemented!")
    }


    @Override
    void processEach(PDProcedure<T> func) {
        if (func == null)
            throw new NullPointerException("The specified function is 'null'")
        def action = new PDProcessAction<T>(tc, func)
        computeResults(dataSourceIteratorProvider, transformations, action)
    }

    @Override
    void close() {
    }

/**
 * Compute a PD set of transformations and  a final action based on the specified data source
 * provider. To compute the final results, it uses the given number of threads.
 *
 * @param provider The data source iterator provider.
 * @param transformations The set of transformations to apply to original data.
 * @param action The final action to retrieve the computed data.
 * @return The requested results.
 */
    protected
    def computeFinalResults(PDResultsStorageManager storageManager, ImmutableDataSourceIteratorProvider<T> provider, List<PDTransformation> transformations, PDAction action, CacheType cacheType) {

        def dsIterator = provider.iterator()

        def internalFinalResults = null

        // Iterate over the collection of data.
        while (true) {
            // First buffering items to be processed.
            List processingBuffer = []
            while (dsIterator.hasNext()) {
                if (processingBuffer.size() >= maxPartitionSize)
                    break
                processingBuffer.add(dsIterator.next())
            }
            if (processingBuffer.size() == 0)
            // End. Processed all items.
                break

            // Processing items. Apply each transformation in the
            // order declared by the programmer.
            boolean mustBreak = false
            GParsPool.withExistingPool(tc.runtime.orchestrator.dataParallelismPool) {
                AbstractPAWrapper parallelBuffer = PDGparsParallelEnhancer.getParallel(processingBuffer)
                for (def t : transformations) {
                    parallelBuffer = t.applyTransformation(parallelBuffer)
                }

                // Apply final action.
                def partialResults = action.applyAction(parallelBuffer)

                // Merge partial results.
                internalFinalResults = action.mergeResults(storageManager, partialResults, internalFinalResults, cacheType)
                if (!action.needMoreResults(internalFinalResults))
                    mustBreak = true
            }
            if (mustBreak)
                break
        }

        return action.getFinalResults(storageManager, internalFinalResults)
    }


    protected
    def computeResults(ImmutableDataSourceIteratorProvider<T> provider, List<PDBaseTransformation> transformations, PDAction action) {
        List<List<PDBaseTransformation>> transformationsSplits = []
        List providerToDelete = []

        // Create new temporary storage manager.
        PDResultsStorageManager storageManager = tc.runtime.pdResultsStorageManagerProvider.createStorageManager(tc.runtime.pdResultsStorageManagerProvider.generateUniqueStorageManagerID())
        tc.runtime.logManager.getLogger("DEBUG").debug("Created storage manager: ${storageManager}")

        // First compute all intermediate results...
        ImmutableDataSourceIteratorProvider currentProvider = computeAllIntermediateResults(storageManager, provider, transformations, providerToDelete, transformationsSplits, CacheType.ON_DISK, false)

        // and then generate final results.
        List<PDBaseTransformation> tr = transformationsSplits[transformationsSplits.size() - 1]
        List<PDTransformation> toProcess = []
        tr.each { toProcess.add((PDTransformation) it) }
        def results = computeFinalResults(storageManager, currentProvider, toProcess, action, CacheType.ON_DISK)
        /*if (providerToDelete.size() > 0) {
            def st = providerToDelete.get(0).storage
            if (st instanceof PDResultsCollectionStorage)
                storageManager.deleteCollectionStorage(st.storageID)
            else if (st instanceof PDResultsMapStorage)
                storageManager.deleteMapStorage(st.storageID)
            else
                storageManager.deleteSortedSetStorage(st.storageID)
            providerToDelete.remove(0)
        }*/

        // Delete temporary storage manager.
        tc.runtime.pdResultsStorageManagerProvider.deleteStorageManager(storageManager.storageManagerID)
        tc.runtime.logManager.getLogger("DEBUG").debug("Deleted storage manager: ${storageManager}")

        results
    }


    protected ImmutableDataSourceIteratorProvider computeIntermediateResults(PDResultsStorageManager storageManager, ImmutableDataSourceIteratorProvider<T> provider, List<PDTransformation> transformations, CacheType cacheType) {
        PDTransformation lastTr = transformations.get(transformations.size() - 1)
        Object internalFinalResults = null
        def dsIterator = provider.iterator()
        // Iterate over the collection of data.
        while (true) {
            // First buffering items to be processed.
            List processingBuffer = new ArrayList(maxPartitionSize)
            while (dsIterator.hasNext()) {
                if (processingBuffer.size() >= maxPartitionSize)
                    break
                processingBuffer.add(dsIterator.next())
            }
            if (processingBuffer.size() == 0)
            // End. Processed all items.
                break

            // Processing items. Apply each transformation in the
            // order declared by the programmer.
            GParsPool.withExistingPool(tc.runtime.orchestrator.dataParallelismPool) {
                AbstractPAWrapper parallelBuffer = PDGparsParallelEnhancer.getParallel(processingBuffer)
                for (def t : transformations) {
                    parallelBuffer = t.applyTransformation(parallelBuffer)
                }

                internalFinalResults = lastTr.mergeResults(storageManager, parallelBuffer, internalFinalResults, cacheType)
            }
        }

        return lastTr.getFinalResults(internalFinalResults)
    }
}
