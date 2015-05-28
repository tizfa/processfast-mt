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
import it.cnr.isti.hlt.processfast.connector.ConnectorMessage

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class GParsConnectorMessage implements ConnectorMessage {

    final Serializable payload
    final DataflowVariable replyTo

    /**
     * Build new instance.
     *
     * @param payload The payload to wrap.
     * @param replyTo If different from 'null', specify the dataflow variable
     * where to reply to sender.
     */
    GParsConnectorMessage(Serializable payload, DataflowVariable replyTo) {
        if (payload == null)
            throw new NullPointerException("The specified payload is 'null'")
        this.payload = payload
        this.replyTo = replyTo
    }


    @Override
    boolean isWaitingReply() {
        return replyTo != null
    }

    @Override
    void replyValue(Serializable v) {
        if (replyTo == null)
            throw new IllegalStateException("This object has not dataflow variable where to reply a message")

    }
}
