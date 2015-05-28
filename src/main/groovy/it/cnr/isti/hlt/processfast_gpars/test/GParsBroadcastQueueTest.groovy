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

package it.cnr.isti.hlt.processfast_gpars.test

import groovyx.gpars.dataflow.DataflowBroadcast
import groovyx.gpars.dataflow.operator.PoisonPill
import groovyx.gpars.group.DefaultPGroup
import it.cnr.isti.hlt.processfast_gpars.connector.GParsBroadcastQueueConnector
import it.cnr.isti.hlt.processfast_gpars.connector.GParsLoadBalancingQueueConnector
import it.cnr.isti.hlt.processfast_gpars.connector.GParsTaskBroadcastQueueConnector
import it.cnr.isti.hlt.processfast_gpars.connector.GParsTaskLoadBalancingQueueConnector

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class GParsBroadcastQueueTest {

    static def main(args) {
        def tasksGroup = new DefaultPGroup()
        // def tasksGroup = new NonDaemonPGroup()
        tasksGroup.with {
            int numConsumers = 3

            DataflowBroadcast dv = new DataflowBroadcast()
            def rc1 = dv.createReadChannel()
            def readChannels = []
            for (int i = 0; i < numConsumers; i++) {
                readChannels << dv.createReadChannel()
            }

            GParsBroadcastQueueConnector sharedQueue = new GParsBroadcastQueueConnector(1)

            Random r = new Random()
            def op1 = operator([rc1], []) {
//            def op1 = task {
                long idOp = r.nextLong()
                println("Producer start ${idOp}")
                GParsTaskBroadcastQueueConnector queue = new GParsTaskBroadcastQueueConnector(sharedQueue, "producer", false, true)
                (1..5).each {
                    queue.putValue(it)
                    println("Producer ${idOp}: val write: ${it}")
                }

                queue.signalEndOfStream()
                (6..8).each {
                    queue.putValue(it)
                    println("Producer ${idOp}: val write: ${it}")
                }
                queue.signalEndOfStream()
                queue.putValue(-1)
                println("Producer ${idOp} ended!")
            }





            def operators = []
            for (int i = 0; i < numConsumers; i++) {
                def op2 = operator([readChannels[i]], []) {
                    //def op2 = task {
                    long idOp = r.nextLong()
                    GParsTaskBroadcastQueueConnector queue = new GParsTaskBroadcastQueueConnector(sharedQueue, "consumer" + idOp, true, false)
                    println("Consumer start ${idOp}")
                    while (true) {
                        def msg = queue.getValue()
                        if (msg == null) {
                            sleep(1)
                            continue
                        }
                        def v = msg.payload
                        if (v == -1)
                            break
                        sleep(new Random().nextInt(500))
                        println("Consumer ${idOp}: val read: ${v}")
                    }

                    println("Consumer ${idOp} ended!")

                    dv << PoisonPill.instance
                }

                operators << op2
            }

            dv << true
            //dv << PoisonPill.instance

            op1.join()
            operators*.join()

        }

        tasksGroup.shutdown()
    }

}
