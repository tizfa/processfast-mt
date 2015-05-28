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

import it.cnr.isti.hlt.processfast.connector.ConnectorType

/**
 * A generic GPars connector used to create communication channels
 * among defined tasks.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class GParsConnector {

    /**
     * The name of the connector (which is unique inside the owning tasks set).
     */
    String connectorName = "Unnamed!"

    /**
     * Indicate if the defined connector is virtual or real.
     */
    boolean isVirtual = false

    /**
     * The connector type. Valid only when the connector is not virtual. Default value is
     * ConnectorType.LOAD_BALANCING_QUEUE
     */
    ConnectorType connectorType = ConnectorType.LOAD_BALANCING_QUEUE


    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false

        GParsConnector that = (GParsConnector) o

        if (connectorName != that.connectorName) return false

        return true
    }

    int hashCode() {
        return connectorName.hashCode()
    }
}
