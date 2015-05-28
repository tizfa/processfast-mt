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
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import groovyx.gpars.dataflow.operator.DataflowProcessor
import groovyx.gpars.dataflow.operator.PoisonPill
import groovyx.gpars.group.DefaultPGroup
import groovyx.gpars.group.NonDaemonPGroup
import groovyx.gpars.remote.BroadcastDiscovery
import it.cnr.isti.hlt.processfast_gpars.connector.GParsLoadBalancingQueueConnector
import it.cnr.isti.hlt.processfast_gpars.connector.GParsTaskLoadBalancingQueueConnector

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class GParsLoadBalancingQueueTest {

    static def main(args) {
        def tasksGroup = new DefaultPGroup(10)
        // def tasksGroup = new NonDaemonPGroup()
        tasksGroup.with {
            int numConsumers = 3

            DataflowBroadcast dv = new DataflowBroadcast()
            def rc1 = dv.createReadChannel()
            def readChannels = []
            for (int i = 0; i < numConsumers; i++) {
                readChannels << dv.createReadChannel()
            }

            GParsLoadBalancingQueueConnector sharedQueue = new GParsLoadBalancingQueueConnector(10)

            def op1 = operator([rc1], []) {
//            def op1 = task {
                GParsTaskLoadBalancingQueueConnector queue = new GParsTaskLoadBalancingQueueConnector(sharedQueue)
                (1..10).each {
                    //sleep(new Random().nextInt(1000))
                    println("Val write: ${it}")
                    queue.putValue(it)

                }

                queue.signalEndOfStream()
                println("Sender ends!")
            }





            def operators = []
            for (int i = 0; i < numConsumers; i++) {
                def op2 = operator([readChannels[i]], []) {
                    //def op2 = task {
                    GParsTaskLoadBalancingQueueConnector queue = new GParsTaskLoadBalancingQueueConnector(sharedQueue)
                    println("Operator start ${i}")
                    while (true) {
                        def msg = queue.getValue()
                        if (msg == null)
                            break
                        def v = msg.payload

                        println("Val read: ${v}")
                    }

                    println("Operator 2 ended!")

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
