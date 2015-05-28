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

package it.cnr.isti.hlt.processfast_java8.data;

import it.cnr.isti.hlt.processfast.core.TaskDataContext;
import it.cnr.isti.hlt.processfast.data.*;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast.utils.Procedure3;
import it.cnr.isti.hlt.processfast_java8.core.GParsTaskContext;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

/**
 * An implementation of a partitionable dataset based on GPars.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class GParsPartitionableDataset<T extends Serializable> implements PartitionableDataset<T> {

    /**
     * The maximum size of a partition (max number of items to process in memory). Default is 1000000.
     */
    protected int maxPartitionSize = 1000000;
    /**
     * The set of transformations to apply.
     */
    protected List<PDBaseTransformation> transformations;
    /**
     * The initial data source iterator.
     */
    ImmutableDataSourceIteratorProvider<T> dataSourceIteratorProvider;

    public GParsTaskContext getTc() {
        return tc;
    }


    /**
     * The GPars task context.
     */
    protected GParsTaskContext tc;

    /**
     * Current storage manager.
     */
    //protected PDResultsStorageManager storageManager
    public GParsPartitionableDataset(GParsTaskContext tc, ImmutableDataSourceIteratorProvider<T> provider) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'");
        if (provider == null)
            throw new NullPointerException("The data source iterator is 'null'");
        this.tc = tc;
        this.dataSourceIteratorProvider = provider;
        this.transformations = new ArrayList<>();
    }


    public GParsPartitionableDataset(GParsPartitionableDataset previousPD) {
        if (previousPD == null)
            throw new NullPointerException("The previous partitionable dataset is 'null'");
        this.transformations = new ArrayList<>();
        this.transformations.addAll(previousPD.transformations);
        this.tc = previousPD.tc;
        this.dataSourceIteratorProvider = previousPD.dataSourceIteratorProvider;
        this.maxPartitionSize = previousPD.maxPartitionSize;
    }


    @Override
    public PartitionableDataset<T> enableLocalComputation(boolean enable) {
        // Ignored on a multi-thread runtime. Always local computation!
        return this;
    }

    @Override
    public PartitionableDataset<T> withPartitionSize(int partitionSize) {
        if (partitionSize < 1)
            throw new IllegalArgumentException("The partition size is invalid: ${partitionSize}");

        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this);
        PDCustomizeTransformation ct = new PDCustomizeTransformation();
        ct.setCustomizationCode((GParsPartitionableDataset pad) -> {
            pad.maxPartitionSize = partitionSize;
        });
        pd.transformations.add(ct);
        return pd;
    }


    protected ImmutableDataSourceIteratorProvider computeAllIntermediateResults(PDResultsStorageManager storageManager, ImmutableDataSourceIteratorProvider<T> provider,
                                                                                List<PDBaseTransformation> transformations,
                                                                                List<ImmutableDataSourceIteratorProvider> providerToDelete, List<List<PDBaseTransformation>> transformationsSplits, CacheType cacheType, boolean computeLast) {
        List<PDBaseTransformation> currentTransformations = new ArrayList<>();

        for (PDBaseTransformation tra : transformations) {
            if (tra.isRealTransformation()) {
                PDTransformation tr = (PDTransformation) tra;
                if (tr.needAllAvailableData()) {
                    currentTransformations.add(tr);
                    transformationsSplits.add(currentTransformations);
                    currentTransformations = new ArrayList<>();
                } else {
                    currentTransformations.add(tr);
                }
            } else {
                // A customization.
                currentTransformations.add(tra);
                transformationsSplits.add(currentTransformations);
                currentTransformations = new ArrayList<>();
            }
        }
        if (currentTransformations.size() > 0)
            transformationsSplits.add(currentTransformations);
        else {
            transformationsSplits.add(new ArrayList<PDBaseTransformation>());
        }

        int numSplits = transformationsSplits.size() - 1;
        if (computeLast)
            numSplits = transformationsSplits.size();

        // First generate in sequence each necessary intermediate results.
        ImmutableDataSourceIteratorProvider currentProvider = provider;
        for (int i = 0; i < numSplits; i++) {
            List<PDBaseTransformation> curTr = transformationsSplits.get(i);
            if (curTr.size() == 0)
                continue;
            PDBaseTransformation last = curTr.get(curTr.size() - 1);
            if (last.isRealTransformation()) {
                List<PDTransformation> toProcess = new ArrayList<>();
                for (PDBaseTransformation bt : curTr)
                    toProcess.add((PDTransformation) bt);
                currentProvider = computeIntermediateResults(storageManager, currentProvider, toProcess, cacheType);
                providerToDelete.add(currentProvider);
            } else { // It is a customization.
                List<PDBaseTransformation> res = curTr.subList(0, curTr.size() - 1);
                if (res.size() > 0) {
                    List<PDTransformation> toProcess = new ArrayList<>();
                    for (PDBaseTransformation bt : res)
                        toProcess.add((PDTransformation) bt);
                    // Compute current results.
                    currentProvider = computeIntermediateResults(storageManager, currentProvider, toProcess, CacheType.ON_DISK);

                    providerToDelete.add(currentProvider);
                }

                // Customize partitionable dataset state.
                PDCustomizeTransformation custTr = (PDCustomizeTransformation) last;
                custTr.getCustomizationCode().call(this);
            }
        }

        return currentProvider;
    }


    @Override
    public PartitionableDataset<T> cache(CacheType cacheType) {
        if (cacheType == null)
            throw new NullPointerException("The cache type is 'null'");
        if (transformations.size() != 0) {
            List<List<PDBaseTransformation>> transformationsSplits = new ArrayList<>();
            List<ImmutableDataSourceIteratorProvider> providerToDelete = new ArrayList<>();
            PDResultsStorageManager storageManager = tc.getRuntime().getPdResultsStorageManagerProvider().createStorageManager(tc.getRuntime().getPdResultsStorageManagerProvider().generateUniqueStorageManagerID());
            ImmutableDataSourceIteratorProvider computedProvider = computeAllIntermediateResults(storageManager, dataSourceIteratorProvider, transformations, providerToDelete, transformationsSplits, cacheType, true);
            return new GParsPartitionableDataset<T>(tc, computedProvider);
        } else {
            return new GParsPartitionableDataset<T>(tc, dataSourceIteratorProvider);
        }
    }

    @Override
    public <Out extends Serializable> PartitionableDataset<Out> map(PDFunction<T, Out> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");

        GParsPartitionableDataset<Out> pd = new GParsPartitionableDataset<Out>(this);
        pd.transformations.add(new PDMapTransformation<T, Out>(tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public <K extends Serializable, V extends Serializable> PairPartitionableDataset<K, V> mapPair(PDPairFunction<T, K, V> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");

        GParsPairPartitionableDataset<K, V> pd = new GParsPairPartitionableDataset<>(this);
        pd.transformations.add(new PDMapPairTransformation<T, K, V>(tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public PartitionableDataset<T> filter(PDFunction<T, Boolean> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");

        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this);
        pd.transformations.add(new PDFilterTransformation<T>(tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public <Out extends Serializable> PartitionableDataset<Out> mapFlat(PDFunction<T, Iterator<Out>> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");

        GParsPartitionableDataset<Out> pd = new GParsPartitionableDataset<Out>(this);
        pd.transformations.add(new PDMapFlatTransformation<>(tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public <K extends Serializable, V extends Serializable> PairPartitionableDataset<K, V> mapPairFlat(PDFunction<T, Iterator<Pair<K, V>>> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");

        GParsPairPartitionableDataset<K, V> pd = new GParsPairPartitionableDataset<K, V>(this);
        pd.transformations.add(new PDMapPairFlatTransformation<>(tc, func, maxPartitionSize));
        return pd;
    }


    @Override
    public PartitionableDataset<T> union(PartitionableDataset<T> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'");
        if (!(dataset instanceof GParsPartitionableDataset))
            throw new IllegalArgumentException("The dataset to intersect must be of type ${GParsPartitionableDataset.class.name}");

        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this);
        pd.transformations.add(new PDUnionTransformation<T>(tc, (GParsPartitionableDataset<T>) dataset, maxPartitionSize));
        return pd;
    }

    @Override
    public <T1 extends Serializable> PartitionableDataset<Pair<T, T1>> pair(PartitionableDataset<T1> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'");
        if (!(dataset instanceof GParsPartitionableDataset))
            throw new IllegalArgumentException("The dataset to intersect must be of type ${GParsPartitionableDataset.class.name}");
        GParsPartitionableDataset<Pair<T, T1>> pd = new GParsPartitionableDataset<Pair<T, T1>>(this);
        pd.transformations.add(new PDPairTransformation<T>(tc, (GParsPartitionableDataset) dataset, maxPartitionSize));
        return pd;
    }

    @Override
    public PartitionableDataset<T> intersection(PartitionableDataset<T> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'");
        if (!(dataset instanceof GParsPartitionableDataset))
            throw new IllegalArgumentException("The dataset to intersect must be of type ${GParsPartitionableDataset.class.name}");

        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this);
        pd.transformations.add(new PDIntersectionTransformation<T>(tc, (GParsPartitionableDataset<T>) dataset, maxPartitionSize));
        return pd;
    }

    @Override
    public PartitionableDataset<T> distinct() {
        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this);
        pd.transformations.add(new PDDistinctTransformation<T>(tc, maxPartitionSize));
        return pd;
    }

    @Override
    public PartitionableDataset<T> sort(boolean sortAscending) {
        GParsPartitionableDataset<T> pd = new GParsPartitionableDataset<T>(this);
        pd.transformations.add(new PDSortTransformation<>(tc, maxPartitionSize, sortAscending));
        return pd;
    }

    @Override
    public <K extends Serializable> PairPartitionableDataset<K, DataIterable<T>> groupBy(PDFunction<T, K> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");
        GParsPairPartitionableDataset<K, DataIterable<T>> pd = new GParsPairPartitionableDataset<>(this);
        pd.transformations.add(new PDGroupByTransformation<T, K>(tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public <U extends Serializable> PairPartitionableDataset<T, U> cartesian(PartitionableDataset<U> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'");
        if (!(dataset instanceof GParsPartitionableDataset))
            throw new IllegalArgumentException("The dataset to use must be of type ${GParsPartitionableDataset.class.name}");

        GParsPairPartitionableDataset<T, U> pd = new GParsPairPartitionableDataset<>(this);
        pd.transformations.add(new PDCartesianTransformation<T>(tc, (GParsPartitionableDataset) dataset, maxPartitionSize));
        return pd;
    }

    @Override
    public T reduce(PDFunction2<T, T, T> func) {
        if (func == null)
            throw new NullPointerException("The programmer's code is 'null'");
        PDReduceAction<T> action = new PDReduceAction<>(tc, func);
        return computeResults(dataSourceIteratorProvider, transformations, action);
    }


    @Override
    public List<T> collect() {
        PDCollectAction<T> action = new PDCollectAction<>(tc);
        Collection results = computeResults(dataSourceIteratorProvider, transformations, action);
        ArrayList<T> ret = new ArrayList<>();
        ret.addAll(results);
        return ret;
    }

    @Override
    public long count() {
        PDCountAction action = new PDCountAction();
        return computeResults(dataSourceIteratorProvider, transformations, action);
    }

    @Override
    public boolean contains(T item) {
        PDContainsAction<T> action = new PDContainsAction<>(tc, item);
        return computeResults(dataSourceIteratorProvider, transformations, action);
    }

    @Override
    public List<T> take(long startFrom, long numItems) {
        if (startFrom < 0)
            throw new IllegalArgumentException("The startFrom value is invalid: ${startFrom}");
        if (numItems < 1)
            throw new IllegalArgumentException("The numItems value is invalid: ${numItems}");

        PDTakeAction<T> action = new PDTakeAction<>(tc, startFrom, numItems);
        return (List<T>) computeResults(dataSourceIteratorProvider, transformations, action);
    }

    @Override
    public PartitionableDataset<T> saveOnStorageManager(Procedure3<TaskDataContext, StorageManager, T> func) {
        // TODO: Add implementation.
        throw new RuntimeException("Not implemented!");
    }


    @Override
    public void processEach(PDProcedure<T> func) {
        if (func == null)
            throw new NullPointerException("The specified function is 'null'");
        PDProcessAction<T> action = new PDProcessAction<T>(tc, func);
        computeResults(dataSourceIteratorProvider, transformations, action);
    }

    @Override
    public void close() {
    }

    /**
     * Compute a PD set of transformations and  a final action based on the specified data source
     * provider. To compute the final results, it uses the given number of threads.
     *
     * @param provider        The data source iterator provider.
     * @param transformations The set of transformations to apply to original data.
     * @param action          The final action to retrieve the computed data.
     * @return The requested results.
     */
    protected <T1> T1 computeFinalResults(PDResultsStorageManager storageManager, ImmutableDataSourceIteratorProvider<T> provider, List<PDTransformation> transformations, PDAction<T1> action, CacheType cacheType) {

        Iterator<T> dsIterator = provider.iterator();

        final Map internalFinalResults = new HashMap<>();

        // Iterate over the collection of data.
        while (true) {
            // First buffering items to be processed.
            List<T> processingBuffer = new ArrayList<>();
            while (dsIterator.hasNext()) {
                if (processingBuffer.size() >= maxPartitionSize)
                    break;
                processingBuffer.add(dsIterator.next());
            }
            if (processingBuffer.size() == 0)
                // End. Processed all items.
                break;

            // Processing items. Apply each transformation in the
            // order declared by the programmer.
            boolean mustBreak = false;


            try {
                tc.getRuntime().getOrchestrator().getDataParallelismPool().submit(() -> {
                    Stream stream = processingBuffer.parallelStream();
                    for (PDTransformation t : transformations) {
                        stream = t.applyTransformation(stream);
                    }

                    // Apply final action.
                    T1 partialResults = action.applyAction(stream);

                    // Merge partial results.
                    action.mergeResults(storageManager, partialResults, internalFinalResults, cacheType);
                }).get();
            } catch (Exception e) {
                throw new RuntimeException("Executing computeFinalResults()", e);
            }

            if (!action.needMoreResults(internalFinalResults))
                mustBreak = true;
            if (mustBreak)
                break;
        }

        T1 finalRes = action.getFinalResults(storageManager, internalFinalResults);
        internalFinalResults.clear();
        return finalRes;
    }


    protected <T1> T1 computeResults(ImmutableDataSourceIteratorProvider<T> provider, List<PDBaseTransformation> transformations, PDAction<T1> action) {
        List<List<PDBaseTransformation>> transformationsSplits = new ArrayList<>();
        List<ImmutableDataSourceIteratorProvider> providerToDelete = new ArrayList<>();

        // Create new temporary storage manager.
        PDResultsStorageManager storageManager = tc.getRuntime().getPdResultsStorageManagerProvider().createStorageManager(tc.getRuntime().getPdResultsStorageManagerProvider().generateUniqueStorageManagerID());
        tc.getRuntime().getLogManager().getLogger("DEBUG").debug("Created storage manager: ${storageManager}");

        // First compute all intermediate results...
        ImmutableDataSourceIteratorProvider currentProvider = computeAllIntermediateResults(storageManager, provider, transformations, providerToDelete, transformationsSplits, CacheType.ON_DISK, false);

        // and then generate final results.
        List<PDBaseTransformation> tr = transformationsSplits.get(transformationsSplits.size() - 1);
        List<PDTransformation> toProcess = new ArrayList<>();
        for (PDBaseTransformation bt : tr)
            toProcess.add((PDTransformation) bt);
        T1 results = computeFinalResults(storageManager, currentProvider, toProcess, action, CacheType.ON_DISK);

        // Delete temporary storage manager.
        tc.getRuntime().getPdResultsStorageManagerProvider().deleteStorageManager(storageManager.getStorageManagerID());
        tc.getRuntime().getLogManager().getLogger("DEBUG").debug("Deleted storage manager: ${storageManager}");

        return results;
    }


    protected ImmutableDataSourceIteratorProvider computeIntermediateResults(PDResultsStorageManager storageManager, ImmutableDataSourceIteratorProvider<T> provider, List<PDTransformation> transformations, CacheType cacheType) {
        PDTransformation lastTr = transformations.get(transformations.size() - 1);
        final Map internalFinalResults = new HashMap<>();
        Iterator<T> dsIterator = provider.iterator();
        // Iterate over the collection of data.
        while (true) {
            // First buffering items to be processed.
            List processingBuffer = new ArrayList(maxPartitionSize);
            while (dsIterator.hasNext()) {
                if (processingBuffer.size() >= maxPartitionSize)
                    break;
                processingBuffer.add(dsIterator.next());
            }
            if (processingBuffer.size() == 0)
                // End. Processed all items.
                break;

            // Processing items. Apply each transformation in the
            // order declared by the programmer.
            try {
                tc.getRuntime().getOrchestrator().getDataParallelismPool().submit(() -> {
                    Stream stream = processingBuffer.parallelStream();
                    for (PDTransformation t : transformations) {
                        stream = t.applyTransformation(stream);
                    }

                    lastTr.mergeResults(storageManager, stream, internalFinalResults, cacheType);
                }).get();
            } catch (Exception e) {
                throw new RuntimeException("Executing computeIntermediateResults()", e);
            }

        }

        PDResultsStorageIteratorProvider itProvider = lastTr.getFinalResults(internalFinalResults);
        internalFinalResults.clear();
        return itProvider;
    }
}
