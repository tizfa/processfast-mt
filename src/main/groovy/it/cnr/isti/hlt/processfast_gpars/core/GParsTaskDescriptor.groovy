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
import it.cnr.isti.hlt.processfast.utils.Function2
import it.cnr.isti.hlt.processfast.utils.Procedure1

/**
 * A GPars task descriptor.
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsTaskDescriptor implements TaskDescriptor {

    /**
     * The task wrapped by this descriptor.
     */
    final Task taskCode

    /**
     * The code used to connect to available connectors.
     */
    Procedure1<WithConnectorInfo> withConnectorsData = null

    /**
     * The code that declares which barriers will be used by this task.
     */
    Function1<WithBarrierInfo, Iterator<String>> withBarriersCode = null

    /**
     * The code that specifies on which virtual machine execute this task.
     */
    Function1<WithVirtualMachineInfo, String> onVirtualMachineCode = null

    /**
     * The number of task instances to create.
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
     * The input data dictionary used to compute task private data dictionary.
     */
    Dictionary taskInputDataDictionary = new RamDictionary()

    /**
     * The code specified by user which generates the private data of the task.
     */
    Function1<WithDataDictionaryInfo, Dictionary> withDataDictionaryCode = null


    GParsTaskDescriptor(Task task) {
        if (task == null)
            throw new NullPointerException("The specified task is 'null'")
        this.taskCode = task
    }

    @Override
    TaskDescriptor withConnectors(Procedure1<WithConnectorInfo> func) {
        if (func == null)
            throw new NullPointerException("The specified function is 'null'")
        this.withConnectorsData = func
        this
    }


    @Override
    TaskDescriptor withBarriers(Function1<WithBarrierInfo, Iterator<String>> barriers) {
        if (barriers == null)
            throw new NullPointerException("The specified function is 'null'")
        withBarriersCode = barriers
        this
    }

    @Override
    TaskDescriptor onVirtualMachine(Function1<WithVirtualMachineInfo, String> machineSelector) {
        if (machineSelector == null)
            throw new NullPointerException("The specified function is 'null'")
        this.onVirtualMachineCode = machineSelector
        this
    }

    @Override
    TaskDescriptor withNumInstances(int minNumInstances, int maxNumInstances) {
        if (minNumInstances <= 0)
            throw new IllegalArgumentException("The minimum number of requested instance is invalid: ${minNumInstances}")
        if (minNumInstances > maxNumInstances)
            throw new IllegalArgumentException("The minimum number of instances is greater than the maximum number of instances: ${minNumInstances} > ${maxNumInstances}")
        this.numInstances = minNumInstances
        this
    }

    @Override
    TaskDescriptor withName(Function1<Integer, String> nameSelector) {
        if (nameSelector == null)
            throw new NullPointerException("The specified function is 'null'")
        this.withNameCode = nameSelector
        this
    }


    @Override
    TaskDescriptor withDataComputationalResourcesPriority(int priority) {
        if (priority < 1)
            throw new IllegalArgumentException("The priority must be greater equals than 1. Current value: ${priority}")
        dataComputationalResourcesPriority = priority
        this
    }

    @Override
    TaskDescriptor withDataDictionary(Dictionary dictionary, Function1<WithDataDictionaryInfo, Dictionary> func) {
        if (dictionary == null)
            throw new NullPointerException("The input data dictionary is 'null'")
        if (func == null)
            throw new NullPointerException("The data dictionary code is 'null'")
        taskInputDataDictionary = dictionary
        withDataDictionaryCode = func
        this
    }

    @Override
    TaskDescriptor withDataDictionary(Function1<WithDataDictionaryInfo, Dictionary> func) {
        return withDataDictionary(new RamDictionary(), { wddi ->
            return func.call(wddi)
        } as Function1<WithDataDictionaryInfo, Dictionary>);
    }

    @Override
    TaskDescriptor onFailure(Function1<FailureInfo, HardwareFailureAction> func) {
        this
    }
}
