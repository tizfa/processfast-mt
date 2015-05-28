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
import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime
import it.cnr.isti.hlt.processfast.core.TaskContext
import it.cnr.isti.hlt.processfast.core.TaskSet
import it.cnr.isti.hlt.processfast.data.CollectionDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.data.PairPartitionableDataset
import it.cnr.isti.hlt.processfast.utils.Pair
import it.cnr.isti.hlt.processfast_gpars.core.GParsRuntime

import java.util.regex.Pattern

@CompileStatic
class DocumentsAnalyzerWithPDIterations {

    static List<String> readArticles(String filename) {
        def ret = []
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

        // Define the distributor process.
        ts.task { TaskContext tc ->
            Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");
            List<String> articles = readArticles("C:\\tmp\\ProcessFastSeminario\\wiki_00")
            int processed = 1
            Map<String, Integer> mapWords = [:]
            for (String article : articles) {
                String[] a = pattern.split(article)
                PairPartitionableDataset<Integer, String> pd = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<String>(a.toList()))
                List<Pair<String, Integer>> results = (List<Pair<String, Integer>>) pd.mapPair { tdc, Pair<Integer, String> item -> new Pair<String, Integer>(item.v2.toLowerCase(), 1) }
                        .reduceByKey { tdc, Integer item1, Integer item2 -> item1 + item2 }
                        .filter { tdc, Pair<String, Integer> item -> item.v2 >= 3 && item.v1.length() > 3 }
                        .collect()
                for (Pair<String, Integer> item : results) {
                    if (mapWords.containsKey(item.v1)) {
                        mapWords.put(item.v1, mapWords.get(item.v1) + item.v2)
                    } else {
                        mapWords.put(item.v1, item.v2)
                    }
                }

                processed++
                if (processed % 50 == 0)
                    tc.logManager.getLogger("test").info("Analyzed ${processed} documents...")
            }

            // Write results.
            StringBuilder sb = new StringBuilder()
            mapWords = mapWords.sort()
            mapWords.each { k, v ->
                sb.append("Word: " + k + " Occurrences: " + v + "\n")
            }
            new File("C:\\tmp\\ProcessFastSeminario\\results.txt").text = sb.toString()
            tc.logManager.getLogger("test").info("Done!")
        }
        .withName { idInstance -> "Distributor" }.withNumInstances(1, 1)



        return ts
    }


    static def main(args) {
        GParsRuntime runtime = new GParsRuntime()
        runtime.numThreadsForDataParallelism = 1
        def ts = createMainTasksSet(runtime)
        runtime.run(ts)
    }

}