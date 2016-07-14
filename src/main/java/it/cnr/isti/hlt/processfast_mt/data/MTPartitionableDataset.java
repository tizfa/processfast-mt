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
import java.util.*;
import java.util.stream.Stream;

/**
 * An implementation of a partitionable dataset based on GPars.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class MTPartitionableDataset<T extends Serializable> implements PartitionableDataset<T> {

    /**
     * The maximum size of a partition (max number of items to process in memory). Default is 100000.
     */
    protected int maxPartitionSize = 10000;

    /**
     * The set of transformations to apply.
     */
    protected List<PDBaseTransformation> transformations;
    /**
     * The GPars task context.
     */
    protected MTTaskContext tc;
    /**
     * The initial data source iterator.
     */
    ImmutableDataSourceIteratorProvider<T> dataSourceIteratorProvider;

    HashMap<String, Serializable> inputValues;


    /**
     * Used to indicate if a processing phase started in {@link #computeResults(ImmutableDataSourceIteratorProvider, List, PDAction)} or
     * in {@link #cache(CacheType)} methods must submit the code to be executed to system thread pool (value "true") with method submit() or
     * assuming that a thread pool is already active and then exploiting it for processing purposes (value "false").
     */
    boolean activateSystemThreadPool;


    /**
     * Current storage manager.
     */
    //protected PDResultsStorageManager storageManager
    public MTPartitionableDataset(MTTaskContext tc, ImmutableDataSourceIteratorProvider<T> provider) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'");
        if (provider == null)
            throw new NullPointerException("The data source iterator is 'null'");
        this.tc = tc;
        this.dataSourceIteratorProvider = provider;
        this.transformations = new ArrayList<>();
        this.activateSystemThreadPool = true;
        this.inputValues = new HashMap<>();
    }

    public MTPartitionableDataset(MTPartitionableDataset previousPD) {
        if (previousPD == null)
            throw new NullPointerException("The previous partitionable dataset is 'null'");
        this.transformations = new ArrayList<>();
        this.transformations.addAll(previousPD.transformations);
        this.tc = previousPD.tc;
        this.dataSourceIteratorProvider = previousPD.dataSourceIteratorProvider;
        this.maxPartitionSize = previousPD.maxPartitionSize;
        this.activateSystemThreadPool = previousPD.activateSystemThreadPool;
        this.inputValues = new HashMap<>();
        this.inputValues.putAll(previousPD.inputValues);
    }

    public MTTaskContext getTc() {
        return tc;
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

        MTPartitionableDataset<T> pd = new MTPartitionableDataset<T>(this);
        PDCustomizeTransformation ct = new PDCustomizeTransformation();
        ct.setCustomizationCode((MTPartitionableDataset pad) -> {
            pad.maxPartitionSize = partitionSize;
        });
        pd.transformations.add(ct);
        return pd;
    }

    @Override
    public PartitionableDataset<T> withInputData(String key, Serializable value) {
        if (key == null || key.isEmpty())
            throw new IllegalArgumentException("The key value is 'null' or empty");
        if (value == null)
            throw new IllegalArgumentException("The input value is 'null'");

        MTPartitionableDataset<T> pd = new MTPartitionableDataset<T>(this);
        PDCustomizeTransformation ct = new PDCustomizeTransformation();
        ct.setCustomizationCode((MTPartitionableDataset pad) -> {
            pad.inputValues.put(key, value);
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


    protected PartitionableDataset<T> cacheInternal(CacheType cacheType, boolean activateSystemThreadPool) {
        if (transformations.size() != 0) {
            List<List<PDBaseTransformation>> transformationsSplits = new ArrayList<>();
            List<ImmutableDataSourceIteratorProvider> providerToDelete = new ArrayList<>();
            PDResultsStorageManager storageManager = tc.getRuntime().getPdResultsStorageManagerProvider().createStorageManager(tc.getRuntime().getPdResultsStorageManagerProvider().generateUniqueStorageManagerID());
            ImmutableDataSourceIteratorProvider computedProvider = computeAllIntermediateResults(storageManager, dataSourceIteratorProvider, transformations, providerToDelete, transformationsSplits, cacheType, true);
            MTPartitionableDataset<T> pd = new MTPartitionableDataset<T>(tc, computedProvider);
            pd.maxPartitionSize = maxPartitionSize;
            pd.activateSystemThreadPool = activateSystemThreadPool;
            // Delete temporary storage manager.
            tc.getRuntime().getPdResultsStorageManagerProvider().deleteStorageManager(storageManager.getStorageManagerID());
            return pd;
        } else {
            MTPartitionableDataset<T> pd = new MTPartitionableDataset<T>(tc, dataSourceIteratorProvider);
            pd.activateSystemThreadPool = activateSystemThreadPool;
            pd.maxPartitionSize = maxPartitionSize;
            return pd;
        }
    }


    @Override
    public PartitionableDataset<T> cache(CacheType cacheType) {
        if (cacheType == null)
            throw new NullPointerException("The cache type is 'null'");
        try {
            if (activateSystemThreadPool) {
                final PartitionableDataset<T> pd = tc.getRuntime().getOrchestrator().getDataParallelismPool().submit(() -> {
                    //return cacheInternal(cacheType, false);
                    return cacheInternal(cacheType, true);
                }).get();
                return pd;
            } else {
                return cacheInternal(cacheType, false);
            }
        } catch (Exception e) {
            throw new RuntimeException("Caching results", e);
        }
    }

    @Override
    public <Out extends Serializable> PartitionableDataset<Out> map(PDFunction<T, Out> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");

        MTPartitionableDataset<Out> pd = new MTPartitionableDataset<Out>(this);
        pd.transformations.add(new PDMapTransformation<>(tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public <K extends Serializable, V extends Serializable> PairPartitionableDataset<K, V> mapPair(PDPairFunction<T, K, V> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");

        MTPairPartitionableDataset<K, V> pd = new MTPairPartitionableDataset<>(this);
        pd.transformations.add(new PDMapPairTransformation<T, K, V>(tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public PartitionableDataset<T> filter(PDFunction<T, Boolean> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");

        MTPartitionableDataset<T> pd = new MTPartitionableDataset<T>(this);
        pd.transformations.add(new PDFilterTransformation<T>(tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public <Out extends Serializable> PartitionableDataset<Out> mapFlat(PDFunction<T, Iterator<Out>> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");

        MTPartitionableDataset<Out> pd = new MTPartitionableDataset<Out>(this);
        pd.transformations.add(new PDMapFlatTransformation<>(tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public <K extends Serializable, V extends Serializable> PairPartitionableDataset<K, V> mapPairFlat(PDFunction<T, Iterator<Pair<K, V>>> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");

        MTPairPartitionableDataset<K, V> pd = new MTPairPartitionableDataset<K, V>(this);
        pd.transformations.add(new PDMapPairFlatTransformation<>(tc, func, maxPartitionSize));
        return pd;
    }


    @Override
    public PartitionableDataset<T> union(PartitionableDataset<T> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'");
        if (!(dataset instanceof MTPartitionableDataset))
            throw new IllegalArgumentException("The dataset to intersect must be of type ${GParsPartitionableDataset.class.name}");

        MTPartitionableDataset<T> pd = new MTPartitionableDataset<T>(this);
        pd.transformations.add(new PDUnionTransformation<T>(tc, (MTPartitionableDataset<T>) dataset, maxPartitionSize));
        return pd;
    }

    @Override
    public <T1 extends Serializable> PartitionableDataset<Pair<T, T1>> pair(PartitionableDataset<T1> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'");
        if (!(dataset instanceof MTPartitionableDataset))
            throw new IllegalArgumentException("The dataset to intersect must be of type ${GParsPartitionableDataset.class.name}");
        MTPartitionableDataset<Pair<T, T1>> pd = new MTPartitionableDataset<Pair<T, T1>>(this);
        pd.transformations.add(new PDPairTransformation<T>(tc, (MTPartitionableDataset) dataset, maxPartitionSize));
        return pd;
    }

    @Override
    public PartitionableDataset<T> intersection(PartitionableDataset<T> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'");
        if (!(dataset instanceof MTPartitionableDataset))
            throw new IllegalArgumentException("The dataset to intersect must be of type ${GParsPartitionableDataset.class.name}");

        MTPartitionableDataset<T> pd = new MTPartitionableDataset<T>(this);
        pd.transformations.add(new PDIntersectionTransformation<T>(tc, (MTPartitionableDataset<T>) dataset, maxPartitionSize));
        return pd;
    }

    @Override
    public PartitionableDataset<T> distinct() {
        MTPartitionableDataset<T> pd = new MTPartitionableDataset<T>(this);
        pd.transformations.add(new PDDistinctTransformation<T>(tc, maxPartitionSize));
        return pd;
    }

    @Override
    public PartitionableDataset<T> sort(boolean sortAscending) {
        MTPartitionableDataset<T> pd = new MTPartitionableDataset<T>(this);
        pd.transformations.add(new PDSortTransformation<>(tc, maxPartitionSize, sortAscending));
        return pd;
    }

    @Override
    public <K extends Serializable> PairPartitionableDataset<K, DataIterable<T>> groupBy(PDFunction<T, K> func) {
        if (func == null)
            throw new NullPointerException("The function code is 'null'");
        MTPairPartitionableDataset<K, DataIterable<T>> pd = new MTPairPartitionableDataset<>(this);
        pd.transformations.add(new PDGroupByTransformation<T, K>(tc, func, maxPartitionSize));
        return pd;
    }

    @Override
    public <U extends Serializable> PairPartitionableDataset<T, U> cartesian(PartitionableDataset<U> dataset) {
        if (dataset == null)
            throw new NullPointerException("The specified dataset is 'null'");
        if (!(dataset instanceof MTPartitionableDataset))
            throw new IllegalArgumentException("The dataset to use must be of type ${GParsPartitionableDataset.class.name}");

        MTPairPartitionableDataset<T, U> pd = new MTPairPartitionableDataset<>(this);
        pd.transformations.add(new PDCartesianTransformation<T>(tc, (MTPartitionableDataset) dataset, maxPartitionSize));
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

    @Override
    public Serializable getInputData(String key) {
        if (key == null || key.isEmpty())
            throw new IllegalArgumentException("The input data key is 'null' or empty");
        return inputValues.get(key);
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

        final Iterator<T> dsIterator = provider.iterator();

        final Map internalFinalResults = new HashMap<>();


        // Processing items. Apply each transformation in the
        // order declared by the programmer.
        boolean mustBreak = false;

        // Iterate over the collection of data.
        while (true) {
            // First buffering items to be processed.
            ArrayList<T> processingBuffer = new ArrayList<>();
            while (processingBuffer.size() < maxPartitionSize && dsIterator.hasNext()) {
                processingBuffer.add((T) dsIterator.next());
            }
            if (processingBuffer.size() == 0)
                break;

            mustBreak = false;

            try {
                //tc.getRuntime().getOrchestrator().getDataParallelismPool().submit(() -> {
                Stream stream = processingBuffer.parallelStream();
                for (PDTransformation t : transformations) {
                    t.setMaxBufferSize(maxPartitionSize);
                    stream = t.applyTransformation(this, stream);
                }

                // Apply final action.
                T1 partialResults = action.applyAction(this, stream);

                // Merge partial results.
                action.mergeResults(this, storageManager, partialResults, internalFinalResults, cacheType);
                //}).get();
            } catch (Exception e) {
                throw new RuntimeException("Executing computeFinalResults()", e);
            }

            if (!action.needMoreResults(this, internalFinalResults))
                mustBreak = true;
            if (mustBreak)
                break;
        }

        T1 finalRes = action.getFinalResults(this, storageManager, internalFinalResults);
        internalFinalResults.clear();
        return finalRes;
    }

    protected <T1> T1 computeResults_internalStreaming(ImmutableDataSourceIteratorProvider<T> provider, List<PDBaseTransformation> transformations, PDAction<T1> action) {
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
        T1 res = (T1) computeFinalResults(storageManager, currentProvider, toProcess, action, CacheType.ON_DISK);

        // Delete temporary storage manager.
        tc.getRuntime().getPdResultsStorageManagerProvider().deleteStorageManager(storageManager.getStorageManagerID());
        return res;
    }


    protected <T1> T1 computeResults_internal(ImmutableDataSourceIteratorProvider<T> provider, List<PDBaseTransformation> transformations, PDAction<T1> action) {
        if (transformations.size() > 0) {
            return computeResults_internalStreaming(provider, transformations, action);
        } else {
            T1 res = action.computeFinalResultsDirectlyOnDataSourceIteratorProvider(this, provider);
            if (res == null)
                res = computeResults_internalStreaming(provider, transformations, action);
            return res;
        }
    }


    protected <T1> T1 computeResults(ImmutableDataSourceIteratorProvider<T> provider, List<PDBaseTransformation> transformations, PDAction<T1> action) {
        try {
            if (activateSystemThreadPool) {
                final T1 results = tc.getRuntime().getOrchestrator().getDataParallelismPool().submit(() -> {
                    return computeResults_internal(provider, transformations, action);
                }).get();
                return results;
            } else {
                return computeResults_internal(provider, transformations, action);
            }
        } catch (Exception e) {
            throw new RuntimeException("Computing results", e);
        }
    }



    protected ImmutableDataSourceIteratorProvider computeIntermediateResults(PDResultsStorageManager storageManager, ImmutableDataSourceIteratorProvider<T> provider, List<PDTransformation> transformations, CacheType cacheType) {
        PDTransformation lastTr = transformations.get(transformations.size() - 1);
        final Map internalFinalResults = new HashMap<>();
        Iterator<T> dsIterator = provider.iterator();

        // Iterate over the collection of data.
        while (true) {
            // First buffering items to be processed.
            ArrayList<T> processingBuffer = new ArrayList<>();

            while (processingBuffer.size() < maxPartitionSize && dsIterator.hasNext()) {
                processingBuffer.add((T) dsIterator.next());
            }
            if (processingBuffer.size() == 0)
                break;

            // Processing items. Apply each transformation in the
            // order declared by the programmer.
            try {
                //tc.getRuntime().getOrchestrator().getDataParallelismPool().submit(() -> {
                Stream stream = processingBuffer.parallelStream();
                for (PDTransformation t : transformations) {
                    t.setMaxBufferSize(maxPartitionSize);
                    stream = t.applyTransformation(this, stream);
                }

                lastTr.setMaxBufferSize(maxPartitionSize);
                lastTr.mergeResults(this, storageManager, stream, internalFinalResults, cacheType);
                //}).get();
            } catch (Exception e) {
                throw new RuntimeException("Executing computeIntermediateResults()", e);
            }

        }

        PDResultsStorageIteratorProvider itProvider = lastTr.getFinalResults(this, internalFinalResults);
        internalFinalResults.clear();
        return itProvider;
    }

}
