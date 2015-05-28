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
import it.cnr.isti.hlt.processfast.data.CollectionDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.data.PairPartitionableDataset
import it.cnr.isti.hlt.processfast.utils.Pair
import it.cnr.isti.hlt.processfast_gpars.core.GParsRuntime

import java.util.regex.Pattern

@CompileStatic
class DocumentsAnalyzerWithPDStream {

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

        ts.createConnector("DISTRIBUTOR", ConnectorType.LOAD_BALANCING_QUEUE)
        ts.createConnector("COLLECTOR", ConnectorType.LOAD_BALANCING_QUEUE)
        ts.createBarrier("WORKER_BARRIER")

        // Distributor.
        ts.task { TaskContext tc ->
            def dist = tc.connectorManager.getConnectorWriter("DISTRIBUTOR")

            (0..11).each { mainDir ->
                (0..99).each {
                    def articles = readArticles("C:\\tmp\\ProcessFastSeminario\\multiplefiles\\${sprintf('%03d', mainDir)}\\wiki_${sprintf('%02d', it)}")
                    dist.putValue(articles)
                }
            }
            dist.signalEndOfStream()
        }.withConnectors { WithConnectorInfo wci -> wci.connectorManager.attachTaskToConnector(wci.taskName, "DISTRIBUTOR", ConnectorCapability.WRITE) }

        // Define the worker process.
        ts.task { TaskContext tc ->
            def dist = tc.connectorManager.getConnectorReader("DISTRIBUTOR")
            def coll = tc.connectorManager.getConnectorWriter("COLLECTOR")
            while (true) {
                def cm = dist.value
                if (cm == null)
                    break

                List<String> articles = (List<String>) cm.payload
                PairPartitionableDataset<Integer, String> pd = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<String>(articles))
                List wordsRes = pd//.withPartitionSize(100)
                        .mapFlat { tdc, Pair<Integer, String> item ->
                    Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])")
                    def a = pattern.split(item.v2).toList()
                    a.iterator()
                }.mapPair { tdc, String item -> new Pair<String, Integer>(item.toLowerCase(), 1) }
                        .reduceByKey { tdc, Integer item1, Integer item2 -> item1 + item2 }
                        .sortByKey(true)
                        .filter { tdc, Pair<String, Integer> item -> item.v2 >= 3 && item.v1.length() > 3 }
                        .collect()

                ArrayList<Pair<String, Integer>> ret = new ArrayList<>()
                ret.addAll(wordsRes)
                coll.putValue(ret)
            }

            tc.barrier("WORKER_BARRIER")
            if (tc.instanceNumber == 0)
                coll.signalEndOfStream()
        }.withNumInstances(4, 8).withBarriers { wbi -> ["WORKER_BARRIER"].iterator() }
                .withConnectors { WithConnectorInfo wci ->
            wci.connectorManager.attachTaskToConnector(wci.taskName, "DISTRIBUTOR", ConnectorCapability.READ)
            wci.connectorManager.attachTaskToConnector(wci.taskName, "COLLECTOR", ConnectorCapability.WRITE)
        }

        // Collector.
        ts.task { TaskContext tc ->
            def coll = tc.connectorManager.getConnectorReader("COLLECTOR")
            TreeMap<String, Integer> wordsRes = new TreeMap<>()
            int numAnalyzed = 0
            while (true) {
                def cm = coll.value
                if (cm == null)
                    break

                List<Pair<String, Integer>> res = (List<Pair<String, Integer>>) cm.payload
                res.each { item ->
                    if (wordsRes.containsKey(item.v1))
                        wordsRes.put(item.v1, wordsRes.get(item.v1) + item.v2)
                    else
                        wordsRes.put(item.v1, item.v2)
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
            tc.logManager.getLogger("test").info("Done!")
        }.withConnectors { WithConnectorInfo wci ->
            wci.connectorManager.attachTaskToConnector(wci.taskName, "COLLECTOR", ConnectorCapability.READ)
        }

        return ts
    }


    static def main(args) {
        GParsRuntime runtime = new GParsRuntime()
        runtime.numThreadsForDataParallelism = 8
        def ts = createMainTasksSet(runtime)
        runtime.run(ts)
    }

}