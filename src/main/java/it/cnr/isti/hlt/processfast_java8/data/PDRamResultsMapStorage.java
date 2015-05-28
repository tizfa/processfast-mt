package it.cnr.isti.hlt.processfast_java8.data;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A RAM implementation of a PD results map storage.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDRamResultsMapStorage<K extends Serializable, V extends Serializable> implements PDResultsMapStorage<K, V> {
    public PDRamResultsMapStorage(String storageID) {
        this(storageID, 10000, 0.3f);
    }

    public PDRamResultsMapStorage(String storageID, int capacity, float loadFactor) {
        if (storageID == null) throw new NullPointerException("The storage ID is 'null'");
        this.storageID = storageID;
        mapValues = new HashMap<K, V>(capacity, loadFactor);
    }

    @Override
    public synchronized void remove(K k) {
        if (k == null) throw new NullPointerException("The specified key is 'null'");
        mapValues.remove("key_" + k);
    }

    @Override
    public synchronized V get(K k) {
        if (k == null) throw new NullPointerException("The specified key is 'null'");
        V ret = mapValues.get(k);
        return ret;
    }

    @Override
    public synchronized void put(K k, V value) {
        if (k == null) throw new NullPointerException("The key value is 'null'");
        if (value == null) throw new NullPointerException("The specified value is 'null'");
        mapValues.put(k, value);
    }

    @Override
    public Iterator<K> keys() {
        return mapValues.keySet().iterator();
    }

    @Override
    public synchronized long size() {
        return mapValues.size();
    }

    @Override
    public synchronized boolean containsKey(K k) {
        return mapValues.containsKey("key_" + k);
    }

    public final String getStorageID() {
        return storageID;
    }

    public final Map<K, V> getMapValues() {
        return mapValues;
    }

    private final String storageID;
    private final Map<K, V> mapValues;
}
