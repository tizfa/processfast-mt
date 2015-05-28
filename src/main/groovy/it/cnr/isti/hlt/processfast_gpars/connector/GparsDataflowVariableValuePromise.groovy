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

package it.cnr.isti.hlt.processfast_gpars.connector

import groovyx.gpars.dataflow.DataflowVariable
import it.cnr.isti.hlt.processfast.connector.ValuePromise

/**
 * A GPars value promise.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GparsDataflowVariableValuePromise implements ValuePromise<Serializable> {

    /**
     * The channel used to retrieve the value.
     */
    final DataflowVariable channelValue

    GparsDataflowVariableValuePromise(DataflowVariable channelValue) {
        if (channelValue == null)
            throw new NullPointerException("The specified dataflow variable is 'null'")
        this.channelValue = channelValue
    }

    @Override
    Serializable get() {
        return channelValue.get()
    }
}
