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

import it.cnr.isti.hlt.processfast.connector.ConnectorType
import it.cnr.isti.hlt.processfast.core.*
import it.cnr.isti.hlt.processfast.data.Dictionary
import it.cnr.isti.hlt.processfast.data.RamDictionary
import it.cnr.isti.hlt.processfast.utils.Function1
import it.cnr.isti.hlt.processfast.utils.Procedure1

/**
 * A tasks set implementation based on GPars.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsTaskSet implements TaskSet {

    /**
     * The set of declared tasks.
     */
    final ArrayList<RunnableDescriptor> tasksDeclared = []

    /**
     * The data dictionary used to exchange data between tasks.
     */
    private final RamDictionary dataDictionary = new RamDictionary()

    /**
     * The set of connectors declared.
     */
    final HashMap<String, ConnectorType> connectorsDeclared = [:]

    /**
     * The size of each declared connector.
     */
    final HashMap<String, Integer> connectorsSizeDeclared = [:]

    /**
     * The set of barriers declared.
     */
    final HashMap<String, String> barriersDeclared = [:]

    /**
     * The initialization code for the tasks set.
     */
    Procedure1<SystemContext> tasksSetInitializationCode = null

    /**
     * The termination code for the tasks set.
     */
    Procedure1<SystemContext> tasksSetTerminationCode = null

    /**
     * The exception handler used for individual tasks exceptions.
     */
    Function1<FailureInfo, HardwareFailureAction> onTaskFailureHandler = null

    /**
     * The set of declared virtual connectors.
     */
    final HashMap<String, String> virtualConnectorsDeclared = [:]

    /**
     * The set of declared virtual barriers.
     */
    final HashMap<String, String> virtualBarriersDeclared = [:]


    @Override
    void createVirtualConnector(String name) {
        if (name == null || name.empty)
            throw new IllegalArgumentException("The connector name is 'null' or empty")
        if (connectorsDeclared.containsKey(name) || virtualConnectorsDeclared.containsKey(name))
            throw new IllegalArgumentException("The connector with name ${name} was already declared")
        virtualConnectorsDeclared.put(name, name)
    }

    @Override
    void createVirtualBarrier(String name) {
        if (name == null || name.empty)
            throw new IllegalArgumentException("The barrier name is 'null' or empty")
        if (barriersDeclared.containsKey(name) || virtualBarriersDeclared.containsKey(name))
            throw new IllegalArgumentException("The barrier with name ${name} was already declared")
        virtualBarriersDeclared.put(name, name)
    }


    TaskDescriptor task(Task task) {
        if (task == null)
            throw new NullPointerException("The specified task is 'null'")
        def td = new GParsTaskDescriptor(task)
        tasksDeclared << td
        return td
    }


    @Override
    void onGenericTaskFailure(Function1<FailureInfo, HardwareFailureAction> func) {
        if (func == null)
            throw new NullPointerException("The specified task exception handler is 'null'")
        onTaskFailureHandler = func
    }


    @Override
    TaskSetDescriptor task(TaskSet tasksSet) {
        if (tasksSet == null)
            throw new NullPointerException("The specified tasks set is 'null'")

        def st = new GParsTaskSetDescriptor(tasksSet)
        tasksDeclared << st
        return st
    }


    @Override
    void createConnector(String name, ConnectorType ctype) {
        if (name == null || name.empty)
            throw new IllegalArgumentException("The connector name is 'null' or empty")
        if (ctype == null)
            throw new NullPointerException("The connector type is 'null'")
        if (connectorsDeclared.containsKey(name))
            throw new IllegalArgumentException("The connector with name ${name} was already declared")
        connectorsDeclared.put(name, ctype)
        connectorsSizeDeclared.put(name, Integer.MAX_VALUE)
    }

    @Override
    void createConnector(String name, ConnectorType ctype, int maxSize) {
        if (name == null || name.empty)
            throw new IllegalArgumentException("The connector name is 'null' or empty")
        if (ctype == null)
            throw new NullPointerException("The connector type is 'null'")
        if (connectorsDeclared.containsKey(name))
            throw new IllegalArgumentException("The connector with name ${name} was already declared")
        if (maxSize < 1)
            throw new IllegalArgumentException("The maximum storage size of connector ${name} is invalid: ${maxSize}")
        connectorsDeclared.put(name, ctype)
        connectorsSizeDeclared.put(name, maxSize)
    }

    @Override
    void createBarrier(String name) {
        if (name == null || name.empty)
            throw new IllegalArgumentException("The barrier name is 'null' or empty")
        barriersDeclared.put(name, name)
    }

    @Override
    void onTasksSetInitialization(Procedure1<SystemContext> func) {
        if (func == null)
            throw new NullPointerException("The specified tasks set initialization code is 'null'")
        tasksSetInitializationCode = func
    }

    @Override
    void onTasksSetTermination(Procedure1<SystemContext> func) {
        if (func == null)
            throw new NullPointerException("The specified tasks set termination code is 'null'")
        tasksSetTerminationCode = func
    }

    @Override
    Dictionary getDataDictionary() {
        return dataDictionary
    }
}
