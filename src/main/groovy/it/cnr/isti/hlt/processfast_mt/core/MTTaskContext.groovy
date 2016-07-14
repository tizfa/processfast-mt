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

package it.cnr.isti.hlt.processfast_mt.core

import groovy.transform.CompileStatic
import it.cnr.isti.hlt.processfast.connector.FutureValuePromise
import it.cnr.isti.hlt.processfast.connector.TaskConnectorManager
import it.cnr.isti.hlt.processfast.connector.ValuePromise
import it.cnr.isti.hlt.processfast.core.AtomicGetOperationsSet
import it.cnr.isti.hlt.processfast.core.AtomicOperationsSet
import it.cnr.isti.hlt.processfast.core.CheckpointDataInfo
import it.cnr.isti.hlt.processfast.core.TaskContext
import it.cnr.isti.hlt.processfast.core.TaskDataContext
import it.cnr.isti.hlt.processfast.data.CacheType
import it.cnr.isti.hlt.processfast.data.DataIterable;
import it.cnr.isti.hlt.processfast.data.Dictionary
import it.cnr.isti.hlt.processfast.data.ImmutableDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.data.PairPartitionableDataset
import it.cnr.isti.hlt.processfast.data.PartitionableDataset
import it.cnr.isti.hlt.processfast.data.RamDictionary
import it.cnr.isti.hlt.processfast.data.ReadableDictionary
import it.cnr.isti.hlt.processfast.utils.Pair
import it.cnr.isti.hlt.processfast_mt.connector.MTBarrier
import it.cnr.isti.hlt.processfast_mt.data.MTPairPartitionableDataset
import it.cnr.isti.hlt.processfast_mt.data.MTPartitionableDataset

import java.util.concurrent.locks.ReadWriteLock

/**
 * A GPars task context.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class MTTaskContext extends MTSystemContext implements TaskContext {

    /**
     * The tasks set containing this task.
     */
    final MTRunningTasksSet runningTasksSet

    /**
     * The task wrapped by this context.
     */
    final MTRunningTask runningTask


    private final MTTaskConnectorManager cm

    MTTaskContext(MTProcessfastRuntime runtime, MTRunningTasksSet tasksSet, MTRunningTask task) {
        super(runtime)

        if (tasksSet == null)
            throw new NullPointerException("The specified tasks set is 'null'")
        if (task == null)
            throw new NullPointerException("The specified task is 'null'")
        this.runningTasksSet = tasksSet
        this.runningTask = task
        this.cm = new MTTaskConnectorManager(tasksSet, task)
    }

    @Override
    int getInstanceNumber() {
        return runningTask.numInstance
    }

    @Override
    int getNumTotalInstances() {
        return runningTask.numTotalInstances
    }

    @Override
    String getTaskName() {
        return runningTask.taskName
    }

    @Override
    void makeCheckpoint(String checkpointName, List<CheckpointDataInfo> data) {

    }

    @Override
    void loadCheckpoint(String checkpointName) {

    }

    @Override
    void deleteCheckpoint(String checkpointName) {

    }

    @Override
    void barrier(String barrierName) {
        if (!runningTask.barriersDeclared.contains(barrierName))
            throw new IllegalArgumentException("The task ${taskName} has not declared access to barrier <${barrierName}>!")
        MTBarrier b = runningTasksSet.barriers.get(barrierName)
        if (b == null) {
            String realBarrier = runningTasksSet.virtualBarriers.get(barrierName)
            if (realBarrier == null)
                throw new RuntimeException("Bug in code. Check the problem!")
            b = runningTasksSet.tasksSetParent.barriers.get(realBarrier)
            if (b == null)
                throw new RuntimeException("Bug in code. Check the problem!")
        }

        b.waitOnBarrier()
    }


    @Override
    def <T extends Serializable> PartitionableDataset<T> createPartitionableDataset(ImmutableDataSourceIteratorProvider<T> dataSource) {
        return new MTPartitionableDataset<T>(this, dataSource)
    }

    @Override
    def <K extends Serializable, V extends Serializable> PairPartitionableDataset<K, V> createPairPartitionableDataset(ImmutableDataSourceIteratorProvider<Pair<K, V>> dataSource) {
        return new MTPairPartitionableDataset<K, V>(this, dataSource)
    }

    @Override
    def <V extends Serializable> PairPartitionableDataset<Integer, DataIterable<V>> createPairPartitionableDataset(Iterator<ImmutableDataSourceIteratorProvider<V>> dataSources) {
        if (!dataSources.hasNext())
            throw new IllegalArgumentException("The data sources iterator is empty")

        ImmutableDataSourceIteratorProvider<V> ds = dataSources.next()
        def dsPD = createPartitionableDataset(ds);
        int curIdx = 0
        PairPartitionableDataset<Integer, V> dsMerged = dsPD.withInputData("idx", curIdx).mapPair({ TaskDataContext ctx, V v ->
            int sourceIdx = (int) ctx.getInputData("idx")
            return new Pair<Integer, V>(sourceIdx, v)
        }).cache(CacheType.ON_DISK)
        while (dataSources.hasNext()) {
            ds = dataSources.next()
            dsPD = createPartitionableDataset(ds)
            curIdx++
            PairPartitionableDataset<Integer, V> dsPair = dsPD.withInputData("idx", curIdx).mapPair({ TaskDataContext ctx, V v ->
                int scourceIdx = (int) ctx.getInputData("idx")
                return new Pair<Integer, V>(scourceIdx, v)
            })
            dsMerged = dsMerged.union(dsPair).cache(CacheType.ON_DISK)
        }

        return dsMerged.groupByKey()
    }

    @Override
    Dictionary getTasksSetDataDictionary() {
        return runningTasksSet.dataDictionary
    }

    @Override
    Dictionary getPrivateTaskDataDictionary() {
        return runningTask.privateDataDictionary
    }

    @Override
    TaskConnectorManager getConnectorManager() {
        return cm
    }


    @Override
    ValuePromise<Void> atomic(String criticalSectionName, ReadableDictionary inputData, AtomicOperationsSet operations) {
        ReadWriteLock lock = runtime.orchestrator.getLock(criticalSectionName)
        AtomicOperationCallable c = new AtomicOperationCallable(criticalSectionName, this, lock, inputData, operations)
        return new FutureValuePromise<Void>(runtime.orchestrator.lockExecutor.submit(c));
    }

    @Override
    ValuePromise<Void> atomic(String criticalSectionName, AtomicOperationsSet operations) {
        return atomic(criticalSectionName, new RamDictionary(), operations);
    }

    @Override
    ValuePromise<ReadableDictionary> atomicGet(String criticalSectionName, ReadableDictionary inputData, AtomicGetOperationsSet operations) {
        ReadWriteLock lock = runtime.orchestrator.getLock(criticalSectionName)
        AtomicGetOperationCallable c = new AtomicGetOperationCallable(criticalSectionName, this, lock, inputData, operations)
        return new FutureValuePromise<ReadableDictionary>(runtime.orchestrator.lockExecutor.submit(c));
    }

    @Override
    ValuePromise<ReadableDictionary> atomicGet(String criticalSectionName, AtomicGetOperationsSet operations) {
        return atomicGet(criticalSectionName, new RamDictionary(), operations);
    }
}
