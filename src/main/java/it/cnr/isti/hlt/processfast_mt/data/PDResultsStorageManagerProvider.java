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

import groovy.transform.CompileStatic;

@CompileStatic
public interface PDResultsStorageManagerProvider {
    /**
     * Create a new storage manager with the specified ID.
     *
     * @param storageManagerID The ID of the storage manager to create.
     * @return The new created storage manager.
     */
    PDResultsStorageManager createStorageManager(String storageManagerID);

    /**
     * Delete the specified storage manager ID.
     *
     * @param storageManagerID
     */
    void deleteStorageManager(String storageManagerID);

    /**
     * Generate unique storage manager ID in the provider.
     *
     * @return An unique storage ID.
     */
    String generateUniqueStorageManagerID();
}
