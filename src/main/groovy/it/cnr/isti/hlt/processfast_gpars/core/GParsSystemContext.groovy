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

package it.cnr.isti.hlt.processfast_gpars.core

import groovy.transform.CompileStatic
import it.cnr.isti.hlt.processfast.core.LogManager
import it.cnr.isti.hlt.processfast.core.SystemContext
import it.cnr.isti.hlt.processfast.data.StorageManager

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
@CompileStatic
class GParsSystemContext implements SystemContext {

    /**
     * The GPars runtime to use.
     */
    final GParsRuntime runtime


    GParsSystemContext(GParsRuntime runtime) {
        if (runtime == null)
            throw new NullPointerException("The GPars runtime is 'null'")

        this.runtime = runtime
    }

    @Override
    LogManager getLogManager() {
        return runtime.orchestrator.internalLogManager
    }

    @Override
    StorageManager getStorageManager() {
        return runtime.storageManager
    }
}
