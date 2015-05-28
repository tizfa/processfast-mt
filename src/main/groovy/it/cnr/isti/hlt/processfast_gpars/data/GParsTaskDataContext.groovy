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

package it.cnr.isti.hlt.processfast_gpars.data

import groovy.transform.CompileStatic
import it.cnr.isti.hlt.processfast.core.TaskDataContext
import it.cnr.isti.hlt.processfast.core.TaskSharedData
import it.cnr.isti.hlt.processfast.data.ReadableDictionary
import it.cnr.isti.hlt.processfast.data.StorageManager
import it.cnr.isti.hlt.processfast_gpars.core.GParsTaskContext

/**
 * A task data context to be used in partitionable datasets
 * operations.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
@CompileStatic
class GParsTaskDataContext implements TaskDataContext {

    final ReadableDictionary tasksSetDataDictionary

    final TaskSharedData taskSharedData

    final StorageManager storageManager

    GParsTaskDataContext(GParsTaskContext tc) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        this.tasksSetDataDictionary = tc.tasksSetDataDictionary
        this.taskSharedData = new GParsTaskSharedData(dataDictionary: tc.privateTaskDataDictionary)
        this.storageManager = tc.storageManager
    }

    @Override
    StorageManager getStorageManager() {
        return storageManager
    }
}


@CompileStatic
class GParsTaskSharedData implements TaskSharedData {

    ReadableDictionary dataDictionary

}
