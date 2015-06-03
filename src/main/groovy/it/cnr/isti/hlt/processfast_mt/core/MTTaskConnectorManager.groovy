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

package it.cnr.isti.hlt.processfast_mt.core

import it.cnr.isti.hlt.processfast.connector.ConnectorReader
import it.cnr.isti.hlt.processfast.connector.ConnectorWriter
import it.cnr.isti.hlt.processfast.connector.TaskConnectorManager
import it.cnr.isti.hlt.processfast_mt.connector.MTLoadBalancingQueueConnector
import it.cnr.isti.hlt.processfast_mt.connector.MTTaskLoadBalancingQueueConnector
import it.cnr.isti.hlt.processfast_mt.connector.MTBroadcastQueueConnector
import it.cnr.isti.hlt.processfast_mt.connector.MTTaskBroadcastQueueConnector

/**
 * A GPars connector manager to be used by running tasks.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class MTTaskConnectorManager implements TaskConnectorManager {

    /**
     * The tasks set which is owner of this task.
     */
    final MTRunningTasksSet tasksSetOwner

    /**
     * The task wrapped by this connection manager.
     */
    final MTRunningTask task


    private HashMap<String, ConnectorReader> taskConnectorsReader = new HashMap<>()
    private HashMap<String, ConnectorWriter> taskConnectorsWriter = new HashMap<>()

    MTTaskConnectorManager(MTRunningTasksSet tasksSet, MTRunningTask task) {
        if (tasksSet == null)
            throw new NullPointerException("The specified tasks set is 'null'")
        if (task == null)
            throw new NullPointerException("The specified task is 'null'")
        this.tasksSetOwner = tasksSet
        this.task = task

        // Create gpars task connectors.
        createConnectors()
    }


    private void createConnectors() {
        def taskName = task.taskName
        tasksSetOwner.virtualConnectors.each { virtualConnectorName, realConnectorName ->
            def ci = tasksSetOwner.tasksSetParent.connectors.get(realConnectorName)
            taskName = tasksSetOwner.tasksSetName + "_" + task.taskName
            storeConnectorInfo(virtualConnectorName, ci, taskName)
        }

        taskName = task.taskName
        tasksSetOwner.connectors.each { connectorName, ci ->
            storeConnectorInfo(connectorName, ci, taskName)
        }
    }


    private void storeConnectorInfo(String connectorName, ConnectorInfo ci, String taskName) {
        boolean readAccess = ci.readAccessList.contains(taskName)
        boolean writeAccess = ci.writeAccessList.contains(taskName)
        if (ci.connector instanceof MTBroadcastQueueConnector) {
            MTTaskBroadcastQueueConnector bq = new MTTaskBroadcastQueueConnector(ci.connector, taskName, readAccess, writeAccess)
            taskConnectorsReader.put(connectorName, bq)
            taskConnectorsWriter.put(connectorName, bq)
        } else if (ci.connector instanceof MTLoadBalancingQueueConnector) {
            MTTaskLoadBalancingQueueConnector lbc = new MTTaskLoadBalancingQueueConnector(ci.connector)
            taskConnectorsReader.put(connectorName, lbc)
            taskConnectorsWriter.put(connectorName, lbc)
        } else
            throw new RuntimeException("Bug in code. The connector type ${ci.connector.class.name} is not handled!")
    }


    @Override
    ConnectorReader getConnectorReader(String name) {
        if (!taskConnectorsReader.containsKey(name))
            return null

        return taskConnectorsReader.get(name)
    }

    @Override
    ConnectorWriter getConnectorWriter(String name) {
        if (!taskConnectorsWriter.containsKey(name))
            return null

        return taskConnectorsWriter.get(name)
    }

    @Override
    ConnectorReader getReadyConnectorReader(List<String> connectors, long maxWaitTime) {
        return null
    }

    @Override
    ConnectorReader getReadyConnectorReaderWithPriority(List<String> connectors, long maxWaitTime) {
        return null
    }
}
