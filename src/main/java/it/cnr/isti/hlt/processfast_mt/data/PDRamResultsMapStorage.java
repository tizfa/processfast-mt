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


import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A RAM implementation of a PD results map storage.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class PDRamResultsMapStorage<K extends Serializable, V extends Serializable> implements PDResultsMapStorage<K, V> {
    public PDRamResultsMapStorage(String storageID) {
        if (storageID == null) throw new NullPointerException("The storage ID is 'null'");
        this.storageID = storageID;
        mapValues = new ConcurrentHashMap<K, V>();
    }


    @Override
    public void remove(K k) {
        if (k == null)
            throw new NullPointerException("The specified key is 'null'");
        mapValues.remove(k);
    }

    @Override
    public V get(K k) {
        if (k == null) throw new NullPointerException("The specified key is 'null'");
        V ret = mapValues.get(k);
        return ret;
    }

    @Override
    public void put(K k, V value) {
        if (k == null) throw new NullPointerException("The key value is 'null'");
        if (value == null) throw new NullPointerException("The specified value is 'null'");
        mapValues.put(k, value);
    }

    @Override
    public Iterator<K> keys() {
        return mapValues.keySet().iterator();
    }

    @Override
    public long size() {
        return mapValues.size();
    }

    @Override
    public boolean containsKey(K k) {
        return mapValues.containsKey(k);
    }

    public final String getStorageID() {
        return storageID;
    }

    private final String storageID;
    private final Map<K, V> mapValues;
}
