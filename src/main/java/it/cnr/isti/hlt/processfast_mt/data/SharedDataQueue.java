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
