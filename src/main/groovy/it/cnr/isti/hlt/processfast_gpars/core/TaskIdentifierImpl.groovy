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

import it.cnr.isti.hlt.processfast.core.TaskIdentifier

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class TaskIdentifierImpl implements TaskIdentifier {
    /**
     * The instance number of the task.
     */
    int instanceNumber = 0

    /**
     * The total number of instances of this task.
     */
    int numTotalInstances = 1

    /**
     * The task name.
     */
    String taskName
}
