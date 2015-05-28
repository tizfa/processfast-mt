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

import groovyx.gpars.group.PGroup
import it.cnr.isti.hlt.processfast.connector.ConnectorMessage

/**
 * A virtual connector reader to be attached to an existing broadcast queue connector.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsBroadcastVirtualConnectorReader {

    /**
     * The source connector.
     */
    final GParsBroadcastQueueConnector sourceConnector

    /**
     * The internal connector ensuring broadcasting behaviour.
     */
    final GParsBroadcastQueueConnector broadcastConnector

    /**
     * The name of this virtual connector.
     */
    String virtualConnectorName

    GParsBroadcastVirtualConnectorReader(GParsBroadcastQueueConnector sourceConnector) {
        if (sourceConnector == null)
            throw new NullPointerException("The source connector is 'null'")
        this.sourceConnector = sourceConnector
        broadcastConnector = new GParsBroadcastQueueConnector()
        this.virtualConnectorName = "${System.currentTimeMillis()}-${new Random().nextInt(10000)}"
    }

    void run(PGroup tasksGroup) {
        GParsTaskBroadcastQueueConnector connector = new GParsTaskBroadcastQueueConnector(broadcastConnector, virtualConnectorName, false, true)
        tasksGroup.operator([], []) {
            while (true) {
                ConnectorMessage msg = sourceConnector.getValue()
                if (msg == null)
                    break

                // Broadcast value to all consumers.
                connector.putValue(msg.payload)
            }
        }
    }
}

