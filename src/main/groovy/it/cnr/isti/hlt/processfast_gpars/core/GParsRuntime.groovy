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

import it.cnr.isti.hlt.processfast.core.HardwareFailureAction
import it.cnr.isti.hlt.processfast.core.LogManager
import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime
import it.cnr.isti.hlt.processfast.core.SLF4JLogManager
import it.cnr.isti.hlt.processfast.core.Task
import it.cnr.isti.hlt.processfast.core.TaskSet
import it.cnr.isti.hlt.processfast.data.Dictionary
import it.cnr.isti.hlt.processfast.data.StorageManager
import it.cnr.isti.hlt.processfast_gpars.data.PDRamResultsStorageManagerProvider
import it.cnr.isti.hlt.processfast_gpars.data.PDResultsStorageManagerProvider

/**
 * A Processfast runtime implementation based on Groovy
 * GPars library.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsRuntime implements ProcessfastRuntime {

    /**
     * The number of threads reserved for data parallel operations. Default value is 5.
     */
    int numThreadsForDataParallelism = 5

    /**
     * The storage managerprovider  for intermediate results in partitionable datasets computations. Default is to store
     * temporary results in RAM.
     */
    PDResultsStorageManagerProvider pdResultsStorageManagerProvider = new PDRamResultsStorageManagerProvider()

    /**
     * The log manager to be used by the runtime. The default implementation is
     * based on SLF4J logging system.
     */
    LogManager logManager

    /**
     * TODO Add a proper default implementation.
     * The storage manager available for the runtime.
     */
    StorageManager storageManager

    /**
     * The runtime component that orchestrate and monitor the running application.
     */
    final GParsProgramOrchestrator orchestrator

    GParsRuntime() {
        orchestrator = new GParsProgramOrchestrator(this)
        logManager = new SLF4JLogManager()
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

        // First initialize the program orchestrator.
        orchestrator.run(tasksSet)
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
        return new GParsTaskSet()
    }

}
