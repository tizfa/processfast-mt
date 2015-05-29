package it.cnr.isti.hlt.processfast_mt.data;

import groovy.transform.CompileStatic;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * A RAM implementation of a PD results storage manager provider.
 * <p>
 * Every created storage (indipendently if requested to be created in
 * RAM or on disk) will be created on RAM.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
public class PDRamResultsStorageManagerProvider implements PDResultsStorageManagerProvider {
    @Override
    public synchronized PDResultsStorageManager createStorageManager(String storageManagerID) {
        if (storageManagerID == null || storageManagerID.isEmpty())
            throw new IllegalArgumentException("The storage manager ID is 'null' or empty");
        PDResultsStorageManager storage = storages.get(storageManagerID);
        if (storage == null) {
            storage = new PDRamResultsStorageManager(storageManagerID);
            storages.put(storageManagerID, (PDRamResultsStorageManager) storage);
        }

        return storage;
    }

    @Override
    public synchronized void deleteStorageManager(String storageManagerID) {
        if (storageManagerID == null || storageManagerID.isEmpty())
            throw new IllegalArgumentException("The storage manager ID is 'null' or empty");
        storages.remove(storageManagerID);
    }

    @Override
    public synchronized String generateUniqueStorageManagerID() {
        final Random r = new Random();
        return String.valueOf(System.currentTimeMillis()) + "_" + String.valueOf(r.nextInt(1000000));
    }


    public void setStorages(Map<String, PDRamResultsStorageManager> storages) {
        this.storages = storages;
    }

    private Map<String, PDRamResultsStorageManager> storages = new LinkedHashMap<String, PDRamResultsStorageManager>();
}
