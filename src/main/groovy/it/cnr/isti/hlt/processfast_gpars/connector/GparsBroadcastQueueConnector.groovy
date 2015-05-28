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

import groovyx.gpars.dataflow.DataflowBroadcast
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowVariable
import it.cnr.isti.hlt.processfast.connector.Connector
import it.cnr.isti.hlt.processfast.connector.ConnectorMessage
import it.cnr.isti.hlt.processfast.connector.ConnectorType
import it.cnr.isti.hlt.processfast.connector.ValuePromise
import it.cnr.isti.hlt.processfast.exception.ConnectorIllegalOperationException

/**
 * A shared broadcasting queue connector based on a dataflow queue. This object is
 * shared among several tasks, so it must be thread-safe.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsBroadcastQueueConnector extends GParsConnector {

    /**
     * The constant used to signal the end of the stream.
     */
    private static final String SIGNAL_STREAM_END = "_signal_stream_end_"

    /**
     * The dataflow broadcast used to implement broadcasting connector.
     */
    final private DataflowBroadcast dataflowBroadcast

    /**
     * Signal if {@link #signalEndOfStream()} has been called.
     */
    synchronized boolean endOfStream = false

    /**
     * The set of registered subscribers.
     */
    private final HashMap<String, DataflowReadChannel> registeredSubscribers

    private final Map<String, Boolean> endedStream

    /**
     * The maximum number of messages allowed on this queue.
     */
    private int maxSize

    GParsBroadcastQueueConnector(int maxSize) {
        if (maxSize < 1)
            throw new IllegalArgumentException("The maximum size is less than 1")
        isVirtual = false
        connectorType = ConnectorType.BROADCAST_QUEUE
        dataflowBroadcast = new DataflowBroadcast()
        registeredSubscribers = [:]
        endedStream = [:]
        this.maxSize = maxSize
    }

    synchronized ConnectorMessage getValue(String taskName) {
        if (!registeredSubscribers.containsKey(taskName))
            throw new IllegalArgumentException("The task name ${taskName} is not registered as reader")

        def msg
        while (true) {
            if (registeredSubscribers.get(taskName).length() >= 1) {
                msg = registeredSubscribers.get(taskName).val
                if (msg == SIGNAL_STREAM_END) {
                    // Wake up sender.
                    notifyAll()
                    continue
                } else
                    break
            } else {
                if (endedStream.get(taskName) == true) {
                    // The stream is closed.
                    return null
                }

                wait()
            }
        }

        // Wake up sender.
        notifyAll()

        return msg
    }

    synchronized void putValue(GParsConnectorMessage v) {
        if (endOfStream) {
            endOfStream = false
            boolean toSleep = true
            while (toSleep) {
                toSleep = false
                registeredSubscribers.keySet().iterator().each { taskName ->
                    if (registeredSubscribers.get(taskName).length() >= maxSize)
                        toSleep = true
                }
                if (toSleep)
                    wait()
            }
            registeredSubscribers.keySet().iterator().each { key ->
                endedStream.put(key, false)
            }
        }
        dataflowBroadcast << v

        notifyAll()
    }


    synchronized void signalEndOfStream() {
        if (endOfStream)
            return
        this.endOfStream = true
        dataflowBroadcast << SIGNAL_STREAM_END
        registeredSubscribers.keySet().iterator().each { key ->
            endedStream.put(key, true)
        }
        notifyAll()
    }


    synchronized void registerSubscriber(GParsTaskBroadcastQueueConnector connector) {
        if (!registeredSubscribers.containsKey(connector.taskName)) {
            registeredSubscribers.put(connector.taskName, dataflowBroadcast.createReadChannel())
            endedStream.put(connector.taskName, false)
        }
    }
}

/**
 * This is the view for a specific task of a connector of type {@link it.cnr.isti.hlt.processfast.connector.ConnectorType#BROADCAST_QUEUE}.
 * Each task has its own view so the object is not shared with other tasks.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsTaskBroadcastQueueConnector implements Connector {

    /**
     * The name of the task.
     */
    final String taskName

    final GParsBroadcastQueueConnector sharedConnector
    final boolean reader
    final boolean writer


    GParsTaskBroadcastQueueConnector(GParsBroadcastQueueConnector sharedConnector, String taskName, boolean isReader, boolean isWriter) {
        if (sharedConnector == null)
            throw new NullPointerException("The shared connector is 'null'")
        if (taskName == null || taskName.empty)
            throw new IllegalArgumentException("The task name is 'null' or empty")

        this.taskName = taskName
        this.sharedConnector = sharedConnector
        if (isReader)
            this.sharedConnector.registerSubscriber(this)
        this.reader = isReader
        this.writer = isWriter
    }

    @Override
    String getConnectorName() {
        return sharedConnector.connectorName
    }

    @Override
    ConnectorMessage getValue() {
        if (!isReader())
            throw new IllegalStateException("The task has not read capabilities")
        return sharedConnector.getValue(taskName)
    }

    @Override
    void putValue(Serializable v) {
        if (!isWriter())
            throw new IllegalStateException("The task has not write capabilities")
        if (v == null)
            throw new NullPointerException("The specified value is 'null'")

        sharedConnector.putValue(new GParsConnectorMessage(v, null))
    }

    @Override
    ValuePromise<Serializable> putValueAndGet(Serializable v) throws ConnectorIllegalOperationException {
        if (!isWriter())
            throw new IllegalStateException("The task has not write capabilities")
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