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

package it.cnr.isti.hlt.processfast_gpars.connector

import groovyx.gpars.dataflow.DataflowVariable
import it.cnr.isti.hlt.processfast.connector.Connector
import it.cnr.isti.hlt.processfast.connector.ConnectorMessage
import it.cnr.isti.hlt.processfast.connector.ConnectorType
import it.cnr.isti.hlt.processfast.connector.ValuePromise
import it.cnr.isti.hlt.processfast.exception.ConnectorIllegalOperationException

/**
 * A shared single value connector based on a dataflow variable. This object is
 * shared among several tasks, so it must be thread-safe.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsSingleValueConnector extends GParsConnector {

    /**
     * The dataflow variable used to implement this specific connector.
     */
    DataflowVariable dataflowVariable = new DataflowVariable()

    /**
     * Signal if {@link #signalEndOfStream()} has been called.
     */
    synchronized boolean endOfStream = false

    GParsSingleValueConnector() {
        isVirtual = false
        connectorType = ConnectorType.SINGLE_VALUE
    }

    synchronized ConnectorMessage getValue() {
        if (endOfStream)
            return null
        else {
            while (!dataflowVariable.isBound() && !endOfStream) {
                wait()
            }
            if (endOfStream)
                return null

            return dataflowVariable.get()
        }
    }

    synchronized void putValue(GParsConnectorMessage v) {
        if (endOfStream) {
            endOfStream = false
            dataflowVariable = new DataflowVariable()
        }
        if (dataflowVariable.isBound() && !endOfStream)
            throw new IllegalStateException("On single value connector, you can write only just one time and read multiple times")
        dataflowVariable << v
        notifyAll()
    }


    synchronized void signalEndOfStream() {
        endOfStream = true
        notifyAll()
    }
}

/**
 * This is the view for a specific task of a connector of type {@link it.cnr.isti.hlt.processfast.connector.ConnectorType#SINGLE_VALUE}.
 * Each task has its own view so the object is not shared with other tasks.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsTaskSingleValueConnector implements Connector {
    /**
     * The name of the task.
     */
    String taskName = "UnnamedTask"

    /**
     * The shared GPars channel used to implement the connector.
     */
    final GParsSingleValueConnector sharedConnector

    GParsTaskSingleValueConnector(GParsSingleValueConnector sharedConnector) {
        if (sharedConnector == null)
            throw new NullPointerException("The shared connector is 'null'")
        this.sharedConnector = sharedConnector
    }

    @Override
    String getConnectorName() {
        return sharedConnector.connectorName
    }

    @Override
    ConnectorMessage getValue() {
        return sharedConnector.getValue()
    }

    @Override
    void putValue(Serializable v) {
        if (v == null)
            throw new NullPointerException("The specified value is 'null'")

        sharedConnector.putValue(new GParsConnectorMessage(v, null))
    }

    @Override
    ValuePromise<Serializable> putValueAndGet(Serializable v) throws ConnectorIllegalOperationException {
        throw new ConnectorIllegalOperationException(taskName, connectorName, "Can not call this method on this type of connector")
    }

    @Override
    void signalEndOfStream() {
        sharedConnector.signalEndOfStream()
    }
}