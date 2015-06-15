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

package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.connector.ConnectorMessage;
import it.cnr.isti.hlt.processfast_mt.connector.MTLoadBalancingQueueConnector;
import it.cnr.isti.hlt.processfast_mt.connector.MTTaskLoadBalancingQueueConnector;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class SharedDataQueue {

    public static class SerializedMessage {
        public long messageSize;
        public Serializable message;
    }

    protected long maxDataInMemory;
    final MTLoadBalancingQueueConnector connector;
    final MTTaskLoadBalancingQueueConnector diskConnector;
    private long counter;

    public SharedDataQueue(long maxDataInMemory) {
        this.maxDataInMemory = maxDataInMemory;
        connector = new MTLoadBalancingQueueConnector(Integer.MAX_VALUE);
        diskConnector = new MTTaskLoadBalancingQueueConnector(connector);
        counter = 0;
    }

    public synchronized void putValue(Serializable value) {

        while (counter > maxDataInMemory) {
            try {
                wait();
            } catch (Exception e) {
            }
        }

        long curValue = getBytesLength(value);
        counter += curValue;
        diskConnector.putValue(value);
        notifyAll();
    }

    public synchronized SerializedMessage getValue() {
        while (counter <= 0) {
            try {
                wait();
            } catch (Exception e) {
            }
        }

        ConnectorMessage cm = diskConnector.getValue();
        notifyAll();
        if (cm == null)
            return null;
        long curValue = getBytesLength(cm.getPayload());
        counter -= curValue;
        SerializedMessage sm = new SerializedMessage();
        sm.messageSize = curValue;
        sm.message = cm.getPayload();
        return sm;
    }

    public synchronized void signalEndOfStream() {
        diskConnector.signalEndOfStream();
    }


    private long getBytesLength(Serializable s) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        long l = 0;
        try {
            ObjectOutputStream dos = new ObjectOutputStream(bos);
            dos.writeObject(s);
            dos.close();
            l = bos.size();
        } catch (Exception e) {
            throw new RuntimeException("Bug in code", e);
        }
        return l;
    }
}
