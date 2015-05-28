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

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import it.cnr.isti.hlt.processfast.connector.Connector
import it.cnr.isti.hlt.processfast.connector.ConnectorMessage
import it.cnr.isti.hlt.processfast.connector.ConnectorType
import it.cnr.isti.hlt.processfast.connector.ValuePromise
import it.cnr.isti.hlt.processfast.exception.ConnectorIllegalOperationException

/**
 * A shared load balancing queue connector based on a dataflow queue. This object is
 * shared among several tasks, so it must be thread-safe.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsLoadBalancingQueueConnector extends GParsConnector {

    /**
     * The constant used to signal the end of the stream.
     */
    private static final String SIGNAL_STREAM_END = "_signal_stream_end_"

    /**
     * The dataflow queue used to implement load-balancing connector.
     */
    private DataflowQueue dataflowQueue = new DataflowQueue()

    /**
     * Signal if {@link #signalEndOfStream()} has been called.
     */
    private boolean endOfStream = false

    /**
     * The maximum number of messages allowed on this queue.
     */
    private int maxSize

    GParsLoadBalancingQueueConnector(int maxSize) {
        if (maxSize < 1)
            throw new IllegalArgumentException("The maximum size is less than 1")
        isVirtual = false
        connectorType = ConnectorType.LOAD_BALANCING_QUEUE
        this.maxSize = maxSize
    }


    synchronized boolean isEndOfStream() {
        return this.@endOfStream
    }

    synchronized ConnectorMessage getValue() {

        while (dataflowQueue.length() == 0 && !endOfStream) {
            wait()
        }

        if (endOfStream) {
            return null
        }

        def msg = dataflowQueue.val
        notifyAll()
        if (msg == SIGNAL_STREAM_END) {
            endOfStream = true
            return null
        }

        return msg
    }

    synchronized void putValue(GParsConnectorMessage v) {
        if (isEndOfStream()) {
            this.endOfStream = false
            this.dataflowQueue = new DataflowQueue()
        }

        while (dataflowQueue.length() >= maxSize) {
            wait()
        }
        this.dataflowQueue << v
        notifyAll()
    }


    synchronized void signalEndOfStream() {
        dataflowQueue << SIGNAL_STREAM_END
        notifyAll()
    }
}

/**
 * This is the view for a specific task of a connector of type {@link it.cnr.isti.hlt.processfast.connector.ConnectorType#LOAD_BALANCING_QUEUE}.
 * Each task has its own view so the object is not shared with other tasks.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsTaskLoadBalancingQueueConnector implements Connector {

    /**
     * The name of the task.
     */
    String taskName = "UnnamedTask"

    final GParsLoadBalancingQueueConnector sharedConnector

    GParsTaskLoadBalancingQueueConnector(GParsLoadBalancingQueueConnector sharedConnector) {
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
        if (v == null)
            throw new NullPointerException("The specified value is 'null'")

        def msg = new GParsConnectorMessage(v, new DataflowVariable())
        sharedConnector.putValue(msg)
        return GparsDataflowVariableValuePromise(msg.replyTo)
    }

    @Override
    void signalEndOfStream() {
        sharedConnector.signalEndOfStream()
    }
}