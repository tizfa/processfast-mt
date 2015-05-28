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

import it.cnr.isti.hlt.processfast.core.*
import it.cnr.isti.hlt.processfast.data.Dictionary
import it.cnr.isti.hlt.processfast.data.RamDictionary
import it.cnr.isti.hlt.processfast.utils.Function1
import it.cnr.isti.hlt.processfast.utils.Pair

/**
 * A GPars tasks set descriptor.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class GParsTaskSetDescriptor implements TaskSetDescriptor {

    /**
     * The code that specifies on which virtual machine execute this task.
     */
    Function1<WithVirtualMachineInfo, String> onVirtualMachineCode = null

    /**
     * The minimum number of task instances to create.
     */
    int numInstances = 1

    /**
     * The function which assign the name of a given instance of this task.
     */
    Function1<Integer, String> withNameCode = null

    /**
     * This sets the relative priority of this task in computational
     * resources allocation for data parallelism. The
     * relative priority is measured among all defined tasks in the tasks set. The default value
     * assigned by the system is 1.
     */
    int dataComputationalResourcesPriority = 1

    /**
     * The code to attach virtual connectors.
     */
    Function1<WithAttachedVirtualConnectorInfo, Iterator<Pair<String, String>>> withAttachedVirtualConnectorsCode = null

    /**
     * The code to attach virtual barriers.
     */
    Function1<WithAttachedVirtualBarrierInfo, Iterator<Pair<String, String>>> withAttachedVirtualBarriersCode = null

    /**
     * The task set represented by this descriptor.
     */
    GParsTaskSet tasksSet = null

    /**
     * The input data dictionary used to compute tasks set private data dictionary.
     */
    Dictionary taskInputDataDictionary = new RamDictionary()

    /**
     * The code specified by user which generates the private data of the tasks set.
     */
    Function1<WithDataDictionaryInfo, Dictionary> withDataDictionaryCode = null

    GParsTaskSetDescriptor(GParsTaskSet tasksSet) {
        if (tasksSet == null)
            throw new NullPointerException("The specified tasks set is 'null'")
        this.tasksSet = tasksSet
    }

    @Override
    TaskSetDescriptor withAttachedVirtualConnectors(Function1<WithAttachedVirtualConnectorInfo, Iterator<Pair<String, String>>> func) {
        if (func == null)
            throw new NullPointerException("The specified function is 'null'")
        this.withAttachedVirtualConnectorsCode = func
        this
    }

    @Override
    TaskSetDescriptor withAttachedVirtualBarriers(Function1<WithAttachedVirtualBarrierInfo, Iterator<Pair<String, String>>> func) {
        if (func == null)
            throw new NullPointerException("The specified function is 'null'")
        this.withAttachedVirtualBarriersCode = func
        this
    }

    @Override
    TaskSetDescriptor onVirtualMachine(Function1<WithVirtualMachineInfo, String> machineSelector) {
        if (machineSelector == null)
            throw new NullPointerException("The specified function is 'null'")
        this.onVirtualMachineCode = machineSelector
        this
    }

    @Override
    TaskSetDescriptor withNumInstances(int minNumInstances, int maxNumInstances) {
        if (minNumInstances <= 0)
            throw new IllegalArgumentException("The number of requested instance is invalid: ${minNumInstances}")
        this.numInstances = minNumInstances
        this
    }

    @Override
    TaskSetDescriptor withName(Function1<Integer, String> nameSelector) {
        if (nameSelector == null)
            throw new NullPointerException("The specified function is 'null'")
        this.withNameCode = nameSelector
        this
    }


    @Override
    TaskSetDescriptor withDataComputationalResourcesPriority(int priority) {
        if (priority < 1)
            throw new IllegalArgumentException("The priority must be greater equals than 1. Current value: ${priority}")
        dataComputationalResourcesPriority = priority
        this
    }

    @Override
    TaskSetDescriptor withDataDictionary(Dictionary dictionary, Function1<WithDataDictionaryInfo, Dictionary> func) {
        if (dictionary == null)
            throw new NullPointerException("The input data dictionary is 'null'")
        if (func == null)
            throw new NullPointerException("The data dictionary code is 'null'")
        taskInputDataDictionary = dictionary
        withDataDictionaryCode = func
        this
    }

    @Override
    TaskSetDescriptor withDataDictionary(Function1<WithDataDictionaryInfo, Dictionary> func) {
        return withDataDictionary(new RamDictionary(), { wddi ->
            return func.call(wddi)
        } as Function1<WithDataDictionaryInfo, Dictionary>)
    }

    @Override
    TaskSetDescriptor onFailure(Function1<FailureInfo, HardwareFailureAction> func) {
        // Unused on GPars.
        return this
    }
}
