package it.cnr.isti.hlt.processfast_mt.data;

import groovy.transform.CompileStatic;

@CompileStatic
public interface PDResultsStorageManagerProvider {
    /**
     * Create a new storage manager with the specified ID.
     *
     * @param storageManagerID The ID of the storage manager to create.
     * @return The new created storage manager.
     */
    public abstract PDResultsStorageManager createStorageManager(String storageManagerID);

    /**
     * Delete the specified storage manager ID.
     *
     * @param storageManagerID
     */
    public abstract void deleteStorageManager(String storageManagerID);

    /**
     * Generate unique storage manager ID in the provider.
     *
     * @return An unique storage ID.
     */
    public abstract String generateUniqueStorageManagerID();
}
