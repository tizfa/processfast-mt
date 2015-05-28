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

import groovy.transform.CompileStatic
import it.cnr.isti.hlt.processfast.connector.ConnectorCapability
import it.cnr.isti.hlt.processfast.connector.ConnectorType
import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime
import it.cnr.isti.hlt.processfast.core.TaskContext
import it.cnr.isti.hlt.processfast.core.TaskSet
import it.cnr.isti.hlt.processfast.core.WithConnectorInfo
import it.cnr.isti.hlt.processfast_gpars.core.GParsRuntime

import java.util.regex.Pattern

@CompileStatic
class DocumentsAnalyzerSequentialStreamAllFiles {

    static ArrayList<String> readArticles(String filename) {
        ArrayList ret = []
        new File(filename).eachLine { String l ->
            if (l.empty)
                return
            if (l.startsWith("<doc") || l.startsWith("</doc"))
                return

            ret << l
        }
        ret
    }

    static TaskSet createMainTasksSet(ProcessfastRuntime runtime) {
        // Create main tasks set.
        def ts = runtime.createTaskSet()

        ts.createConnector("DISTRIBUTOR", ConnectorType.LOAD_BALANCING_QUEUE, 50)
        ts.createConnector("COLLECTOR", ConnectorType.LOAD_BALANCING_QUEUE)
        ts.createBarrier("WORKER_BARRIER")

        // Distributor.
        ts.task { TaskContext tc ->
            def dist = tc.connectorManager.getConnectorWriter("DISTRIBUTOR")
            (0..10).each { mainDir ->
                (0..99).each {
                    def fname = "C:\\tmp\\ProcessFastSeminario\\multiplefiles\\${sprintf('%03d', mainDir)}\\wiki_${sprintf('%02d', it)}".toString()
                    if (new File(fname).exists()) {
                        def articles = readArticles(fname)
                        dist.putValue(articles)
                    }
                }
            }
            dist.signalEndOfStream()
        }.withConnectors { WithConnectorInfo wci -> wci.connectorManager.attachTaskToConnector(wci.taskName, "DISTRIBUTOR", ConnectorCapability.WRITE) }
                .onVirtualMachine { vmi -> "vm" }

        // Define the worker process.
        ts.task { TaskContext tc ->
            def dist = tc.connectorManager.getConnectorReader("DISTRIBUTOR")
            def coll = tc.connectorManager.getConnectorWriter("COLLECTOR")
            def pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");
            while (true) {
                def cm = dist.value
                if (cm == null)
                    break

                List<String> articles = (List<String>) cm.payload
                HashMap<String, Integer> mapWords = new HashMap<String, Integer>(1000000)
                articles.eachWithIndex { article, idx ->
                    def a = pattern.split(article)
                    a.each { String word ->
                        if (word.empty)
                            return
                        def w = word.toLowerCase()
                        if (!mapWords.containsKey(w)) {
                            mapWords.put(w, 1)
                        } else {
                            mapWords.put(w, mapWords.get(w) + 1)
                        }
                    }

                }

                coll.putValue(mapWords)
            }

            tc.barrier("WORKER_BARRIER")
            if (tc.instanceNumber == 0)
                coll.signalEndOfStream()
        }.withNumInstances(8, 15).withBarriers { wbi -> ["WORKER_BARRIER"].iterator() }
                .withConnectors { WithConnectorInfo wci ->
            wci.connectorManager.attachTaskToConnector(wci.taskName, "DISTRIBUTOR", ConnectorCapability.READ)
            wci.connectorManager.attachTaskToConnector(wci.taskName, "COLLECTOR", ConnectorCapability.WRITE)
        }.onVirtualMachine { vmi -> "vm" }

        // Collector.
        ts.task { TaskContext tc ->
            def startTime = System.currentTimeMillis()
            def coll = tc.connectorManager.getConnectorReader("COLLECTOR")
            TreeMap<String, Integer> wordsRes = new TreeMap<>()
            int numAnalyzed = 0
            while (true) {
                def cm = coll.value
                if (cm == null)
                    break

                HashMap<String, Integer> res = (HashMap<String, Integer>) cm.payload
                res.each { k, v ->
                    if (wordsRes.containsKey(k))
                        wordsRes.put(k, wordsRes.get(k) + v)
                    else
                        wordsRes.put(k, v)
                }
                numAnalyzed++
                tc.logManager.getLogger("test").info("Analyzed ${numAnalyzed} file(s)...")
            }

            // Write results.
            StringBuilder sb = new StringBuilder()
            wordsRes.each { k, v ->
                sb.append("Word: " + k + " Occurrences: " + v + "\n")
            }
            new File("C:\\tmp\\ProcessFastSeminario\\multiplefiles\\results.txt").text = sb.toString()
            def endTime = System.currentTimeMillis()
            tc.logManager.getLogger("test").info("Done! Execution time: ${endTime - startTime} milliseconds.")
        }.withConnectors { WithConnectorInfo wci ->
            wci.connectorManager.attachTaskToConnector(wci.taskName, "COLLECTOR", ConnectorCapability.READ)
        }.onVirtualMachine { vmi -> "vm" }

        return ts
    }

    static def main(args) {
        GParsRuntime runtime = new GParsRuntime()
        def ts = createMainTasksSet(runtime)
        runtime.run(ts)
    }
}