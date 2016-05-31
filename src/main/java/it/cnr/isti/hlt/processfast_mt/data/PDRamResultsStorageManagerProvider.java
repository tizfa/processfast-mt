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
import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime;

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

    private ProcessfastRuntime runtime;

    private static final String LOGGER_NAME = "PDRamResultsStorageManagerProvider";

    PDRamResultsStorageManagerProvider(ProcessfastRuntime runtime) {
        if (runtime == null)
            throw new NullPointerException("Runtime environment is 'null'");
        this.runtime = runtime;
    }

    @Override
    public synchronized PDResultsStorageManager createStorageManager(String storageManagerID) {
        if (storageManagerID == null || storageManagerID.isEmpty())
            throw new IllegalArgumentException("The storage manager ID is 'null' or empty");
        PDResultsStorageManager storage = storages.get(storageManagerID);
        if (storage == null) {
            storage = new PDRamResultsStorageManager(storageManagerID);
            storages.put(storageManagerID, (PDRamResultsStorageManager) storage);
            runtime.getLogManager().getLogger(LOGGER_NAME).debug("Created storage manager " + storageManagerID);
        }

        return storage;
    }

    @Override
    public synchronized void deleteStorageManager(String storageManagerID) {
        if (storageManagerID == null || storageManagerID.isEmpty())
            throw new IllegalArgumentException("The storage manager ID is 'null' or empty");
        storages.remove(storageManagerID);
        runtime.getLogManager().getLogger(LOGGER_NAME).debug("Delete storage manager " + storageManagerID);
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
