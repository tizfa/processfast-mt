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

import it.cnr.isti.hlt.processfast.connector.ConnectorCapability
import it.cnr.isti.hlt.processfast.connector.ConnectorType
import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime
import it.cnr.isti.hlt.processfast.core.TaskSet
import it.cnr.isti.hlt.processfast.data.CollectionDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.data.PairPartitionableDataset
import it.cnr.isti.hlt.processfast.utils.Pair
import it.cnr.isti.hlt.processfast_gpars.core.GParsRuntime

import java.util.regex.Pattern


static def readArticles(String filename) {
    def ret = []
    new File(filename).eachLine { l ->
        if (l.empty)
            return
        if (l.startsWith("<doc") || l.startsWith("</doc"))
            return

        ret << l
    }
    ret
}


TaskSet createWorker(ProcessfastRuntime runtime) {
    TaskSet ts = runtime.createTaskSet()

    // Create the required virtual connectors.
    ts.createVirtualConnector("V_DISTRIBUTOR")
    ts.createVirtualConnector("V_COLLECTOR")

    // Create the required real connectors.
    ts.createConnector("DISTRIBUTOR_WORKER", ConnectorType.BROADCAST_QUEUE)
    ts.createConnector("COLLECTOR_WORKER", ConnectorType.LOAD_BALANCING_QUEUE)

    // Create a barrier to synchronize the end of workers.
    ts.createBarrier("workers")

    // Create a virtual barrier to synchronize multiple instances of of this streamable tasks set.
    ts.createVirtualBarrier("ts_document_analyzer_barrier")

    // Document distributor.
    ts.task { tc ->
        def vdistributor = tc.connectorManager.getConnectorReader("V_DISTRIBUTOR")
        def distributorWorker = tc.connectorManager.getConnectorWriter("DISTRIBUTOR_WORKER")
        while (true) {
            def msg = vdistributor.value
            if (msg == null)
            // End of stream.
                break
            // Pass the msg to all workers.
            distributorWorker.putValue(msg.payload)
        }

        // Signal that has no more data to send.
        distributorWorker.signalEndOfStream()

    }.withConnectors { wci ->
        wci.connectorManager.attachTaskToConnector(wci.taskName, "V_DISTRIBUTOR", ConnectorCapability.READ)
        wci.connectorManager.attachTaskToConnector(wci.taskName, "DISTRIBUTOR_WORKER", ConnectorCapability.WRITE)
    }.withName { ni -> "DocumentDistributor" }

    // 1° worker: compute the frequencies of words.
    ts.task { tc ->
        def pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])")
        def dist = tc.connectorManager.getConnectorReader("DISTRIBUTOR_WORKER")
        def coll = tc.connectorManager.getConnectorWriter("COLLECTOR_WORKER")
        while (true) {
            def msg = dist.value
            if (msg == null)
                break
            // Process document text by splitting it into words and then compute, for each word,
            // the number of occurrences. All the words with a number of occurrences < 3 are filtered
            // out.
            def article = msg.payload
            def a = pattern.split(article.text)
            PairPartitionableDataset<Integer, String> pd = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<String>(a.toList()))
            def results = pd.mapPair { tdc, item -> new Pair(item.v2.toLowerCase(), 1) }
                    .reduceByKey { tdc, item1, item2 -> item1 + item2 }
                    .filter { tdc, item -> item.v2 >= 3 && item.v1.length() > 3 }
                    .collect()

            // Send the results (supposing all is serializable).
            def ret = [:]
            ret.idArticle = article.idArticle
            ret.msgType = 0 // 0 for this type of worker.
            ret.results = results
            coll.putValue(ret)
        }
        // Wait for the other worker.
        tc.barrier("workers")

        // Signal to collector that it will receive no more data.
        coll.signalEndOfStream()
    }.withConnectors { wci ->
        wci.connectorManager.attachTaskToConnector(wci.taskName, "DISTRIBUTOR_WORKER", ConnectorCapability.READ)
        wci.connectorManager.attachTaskToConnector(wci.taskName, "COLLECTOR_WORKER", ConnectorCapability.WRITE)
    }.withName { ni -> "Worker1" }.withBarriers { wbi -> ["workers"].iterator() }

    // 2° worker: compute the maximum  word length.
    ts.task { tc ->
        def pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");
        def dist = tc.connectorManager.getConnectorReader("DISTRIBUTOR_WORKER")
        def coll = tc.connectorManager.getConnectorWriter("COLLECTOR_WORKER")
        while (true) {
            def msg = dist.value
            if (msg == null)
                break

            // Process document text by splitting it into words and then compute the maximum word length.
            def article = msg.payload
            def a = pattern.split(article.text)
            PairPartitionableDataset<Integer, String> pd = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<String>(a.toList()))
            def results = pd.filter { tdc, item -> !item.v2.trim().empty }.mapPair { tdc, item -> new Pair(item.v2.length(), item.v2) }.sortByKey(true).collect()
            def maxWordLength = results.get(results.size() - 1).v1

            // Send the results (supposing all is serializable).
            def ret = [:]
            ret.idArticle = article.idArticle
            ret.msgType = 1 // 1 for this type of worker.
            ret.results = [maxWordLength]
            coll.putValue(ret)
        }

        // Wait for the other worker.
        tc.barrier("workers")
    }.withConnectors { wci ->
        wci.connectorManager.attachTaskToConnector(wci.taskName, "DISTRIBUTOR_WORKER", ConnectorCapability.READ)
        wci.connectorManager.attachTaskToConnector(wci.taskName, "COLLECTOR_WORKER", ConnectorCapability.WRITE)
    }.withName { ni -> "Worker2" }.withBarriers { wbi -> ["workers"].iterator() }

    // Collect results.
    ts.task { tc ->
        Map mapWords = [:]
        Map mapAverage = [:]
        def coll = tc.connectorManager.getConnectorReader("COLLECTOR_WORKER")
        def v_coll = tc.connectorManager.getConnectorWriter("V_COLLECTOR")
        while (true) {
            def ret = coll.value
            if (ret == null)
                break
            def msg = ret.payload
            if (msg.msgType == 0) {
                // Update occurrences.
                mapWords.put(msg.idArticle, msg)
            } else {
                // Update lengths.
                mapAverage.put(msg.idArticle, msg)
            }
            if (mapWords.containsKey(msg.idArticle) && mapAverage.containsKey(msg.idArticle)) {
                // Send results only when both types are available.
                def res = [:]
                res.idArticle = msg.idArticle
                res.occurrences = mapWords.get(msg.idArticle).results
                res.avg = mapAverage.get(msg.idArticle).results
                v_coll.putValue(res)
                mapWords.remove(msg.idArticle)
                mapAverage.remove(msg.idArticle)
            }
        }

        tc.barrier("ts_document_analyzer_barrier")
        if (tc.instanceNumber == 0)
        // Signal to external connector that no more data will be sent.
            v_coll.signalEndOfStream()
    }.withConnectors { wci ->
        wci.connectorManager.attachTaskToConnector(wci.taskName, "V_COLLECTOR", ConnectorCapability.WRITE)
        wci.connectorManager.attachTaskToConnector(wci.taskName, "COLLECTOR_WORKER", ConnectorCapability.READ)
    }.withName { ni -> "DocumentCollector" }.withBarriers { wbi -> ["ts_document_analyzer_barrier"].iterator() }

    return ts
}


TaskSet createMainTasksSet(ProcessfastRuntime runtime) {
    // Create main tasks set.
    def ts = runtime.createTaskSet()

    // Create the required connectors.
    ts.createConnector("DISTRIBUTOR", ConnectorType.LOAD_BALANCING_QUEUE)
    ts.createConnector("COLLECTOR", ConnectorType.LOAD_BALANCING_QUEUE)

    // A barrier to synchronize the created set of document analyzers.
    ts.createBarrier("DOCUMENT_ANALYZER_BARRIER")

    // Define the distributor process.
    ts.task { tc ->
        def articles = readArticles("C:\\tmp\\ProcessFastSeminario\\onefile\\wiki_00")
        def connector = tc.connectorManager.getConnectorWriter("DISTRIBUTOR")
        articles.eachWithIndex { article, idx ->
            connector.putValue([idArticle: idx, text: article])
        }
        // Signal that there are no more articles to analyze.
        connector.signalEndOfStream()
    }.withConnectors { wci -> wci.connectorManager.attachTaskToConnector(wci.taskName, "DISTRIBUTOR", ConnectorCapability.WRITE) }
            .withName { idInstance -> "Distributor" }.withNumInstances(1, 1)

    // Declare the "blackbox" worker.
    def bb = createWorker(runtime)
    ts.task(bb).withNumInstances(1, 1)
            .withName { ni -> "DocAnalyzer${ni + 1}" }
            .withAttachedVirtualConnectors { vci ->
        [new Pair("V_DISTRIBUTOR", "DISTRIBUTOR"),
         new Pair("V_COLLECTOR", "COLLECTOR")].iterator()
    }
    .withAttachedVirtualBarriers { vb -> [new Pair("ts_document_analyzer_barrier", "DOCUMENT_ANALYZER_BARRIER")].iterator() }

    // Define the collector.
    ts.task { tc ->
        Map mapWords = [:]
        double avgMaxLength = 0
        def coll = tc.connectorManager.getConnectorReader("COLLECTOR")
        def processed = 1
        while (true) {
            def ret = coll.value
            if (ret == null)
                break
            def msg = ret.payload

            // Update occurrences.
            msg.occurrences.each { item ->
                def v = 0
                if (mapWords.containsKey(item.v1))
                    v = mapWords.get(item.v1)
                mapWords.put(item.v1, v + item.v2)
            }

            // Update lengths.
            avgMaxLength = avgMaxLength + (msg.avg[0] - avgMaxLength) / processed
            if (processed % 50 == 0)
                tc.logManager.getLogger("test").info("Analyzed ${processed} documents...")

            processed++
        }

        // Write results.
        def sb = new StringBuilder()
        sb.append("Average max length: ${avgMaxLength}\n")
        mapWords = mapWords.sort()
        mapWords.each { k, v ->
            sb.append("Word: " + k + " Occurrences: " + v + "\n")
        }
        new File("C:\\tmp\\ProcessFastSeminario\\onefile\\results.txt").text = sb.toString()
        tc.logManager.getLogger("test").info("Done!")
    }.withConnectors { wci -> wci.connectorManager.attachTaskToConnector(wci.taskName, "COLLECTOR", ConnectorCapability.READ) }
            .withName { idInstance -> "Collector" }.withNumInstances(1, 1)

    return ts
}


GParsRuntime runtime = new GParsRuntime()
runtime.numThreadsForDataParallelism = 4
def ts = createMainTasksSet(runtime)
runtime.run(ts)

