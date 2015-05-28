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

package it.cnr.isti.hlt.processfast_gpars.core

import com.sun.org.apache.xalan.internal.xsltc.cmdline.Compile
import groovy.transform.CompileStatic
import it.cnr.isti.hlt.processfast.connector.TaskConnectorManager
import it.cnr.isti.hlt.processfast.core.CheckpointDataInfo
import it.cnr.isti.hlt.processfast.core.TaskContext
import it.cnr.isti.hlt.processfast.data.DataIterable;
import it.cnr.isti.hlt.processfast.data.Dictionary
import it.cnr.isti.hlt.processfast.data.ImmutableDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.data.PairPartitionableDataset
import it.cnr.isti.hlt.processfast.data.PartitionableDataset
import it.cnr.isti.hlt.processfast.utils.Pair
import it.cnr.isti.hlt.processfast_gpars.connector.GParsBarrier
import it.cnr.isti.hlt.processfast_gpars.data.GParsPairPartitionableDataset
import it.cnr.isti.hlt.processfast_gpars.data.GParsPartitionableDataset

/**
 * A GPars task context.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class GParsTaskContext extends GParsSystemContext implements TaskContext {

    /**
     * The tasks set containing this task.
     */
    final GParsRunningTasksSet runningTasksSet

    /**
     * The task wrapped by this context.
     */
    final GParsRunningTask runningTask


    private final GParsTaskConnectorManager cm

    GParsTaskContext(GParsRuntime runtime, GParsRunningTasksSet tasksSet, GParsRunningTask task) {
        super(runtime)

        if (tasksSet == null)
            throw new NullPointerException("The specified tasks set is 'null'")
        if (task == null)
            throw new NullPointerException("The specified task is 'null'")
        this.runningTasksSet = tasksSet
        this.runningTask = task
        this.cm = new GParsTaskConnectorManager(tasksSet, task)
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
        GParsBarrier b = runningTasksSet.barriers.get(barrierName)
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
        return new GParsPartitionableDataset<T>(this, dataSource)
    }

    @Override
    def <K extends Serializable, V extends Serializable> PairPartitionableDataset<K, V> createPairPartitionableDataset(ImmutableDataSourceIteratorProvider<Pair<K, V>> dataSource) {
        return new GParsPairPartitionableDataset<K, V>(this, dataSource)
    }

    @Override
    def <K extends Serializable, V extends Serializable> PairPartitionableDataset<K, DataIterable<V>> createPairPartitionableDataset(Iterator<ImmutableDataSourceIteratorProvider<V>> dataSources) {
        // TODO Must be implemented.
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
}
