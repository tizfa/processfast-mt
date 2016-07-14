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

import it.cnr.isti.hlt.processfast.core.HardwareFailureAction
import it.cnr.isti.hlt.processfast.core.LogManager
import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime
import it.cnr.isti.hlt.processfast.core.SLF4JLogManager
import it.cnr.isti.hlt.processfast.core.Task
import it.cnr.isti.hlt.processfast.core.TaskSet
import it.cnr.isti.hlt.processfast.data.Dictionary
import it.cnr.isti.hlt.processfast.data.StorageManager
import it.cnr.isti.hlt.processfast.data.StorageManagerProvider
import it.cnr.isti.hlt.processfast_mt.data.PDRamResultsStorageManagerProvider
import it.cnr.isti.hlt.processfast_mt.data.PDResultsStorageManagerProvider

import java.util.concurrent.locks.ReadWriteLock


/**
 * A Processfast runtime implementation which exploits multithreading
 * while executing the programs.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class MTProcessfastRuntime implements ProcessfastRuntime {

    /**
     * The number of threads reserved for data parallel operations. Default value is 5.
     */
    int numThreadsForDataParallelism = 5

    /**
     * The storage managerprovider  for intermediate results in partitionable datasets computations. Default is to store
     * temporary results in RAM.
     */
    PDResultsStorageManagerProvider pdResultsStorageManagerProvider

    /**
     * The log manager to be used by the runtime. The default implementation is
     * based on SLF4J logging system.
     */
    LogManager logManager

    /**
     * The storage manager provider available for the runtime.
     */
    StorageManager storageManager

    private StorageManagerProvider storageManagerProvider

    /**
     * The runtime component that orchestrate and monitor the running application.
     */
    final MTProgramOrchestrator orchestrator


    MTProcessfastRuntime() {
        orchestrator = new MTProgramOrchestrator(this)
        logManager = new SLF4JLogManager()
        storageManagerProvider = new it.cnr.isti.hlt.processfast_storage_mapdb.MapDBRamStorageManagerProvider()
        pdResultsStorageManagerProvider = new PDRamResultsStorageManagerProvider(this)
    }

    /**
     * Get the used log manager.
     *
     * @return The used log manager.
     */
    LogManager getLogManager() {
        return this.@logManager
    }


    @Override
    void run(TaskSet tasksSet) {
        if (tasksSet == null)
            throw new NullPointerException("The specified tasks set is 'null'")

        // Open storage manager.
        storageManagerProvider.open();
        try {
            // First initialize the program orchestrator.
            orchestrator.run(tasksSet)
        } finally {
            storageManagerProvider.close()
        }
    }

    @Override
    void run(Dictionary dict, Task task) {
        if (dict == null)
            throw new NullPointerException("The specified data dictionary is 'null'")
        if (task == null)
            throw new NullPointerException("The specified task code is 'null'")


        def ts = createTaskSet()
        ts.task(task).withDataDictionary(dict) { wddi ->
            wddi.dataDictionary
        }.onFailure { fi ->
            HardwareFailureAction.APPLICATION_TERMINATION
        }

        run(ts)

    }

    @Override
    TaskSet createTaskSet() {
        return new MTTaskSet()
    }

    /**
     * Get the storage manager associated with this runtime.
     *
     * @return The storage manager associated to this runtime.
     */
    StorageManager getStorageManager() {
        if (storageManager == null) {
            storageManager = storageManagerProvider.getStorageManager("runtime")
        }
        storageManager
    }


    public setStorageManagerProvider(StorageManagerProvider smp) {
        if (smp == null)
            throw new NullPointerException("The specified storage manager provider is 'null'")
        this.storageManagerProvider = smp;
    }
}
