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
class DocumentsAnalyzerWithPD2 {

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
            long startTime = System.currentTimeMillis()
            List<String> articles = readArticles("C:\\tmp\\ProcessFastSeminario\\onefile\\wiki_00")
            PairPartitionableDataset<Integer, String> pd = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<String>(articles))
            Map<String, Integer> wordsRes = pd.map { tdc, Pair<Integer, String> item ->
                Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])")
                String[] a = pattern.split(item.v2)
                HashMap<String, Integer> localDict = [:]
                a.each { String word ->
                    if (word.length() <= 3)
                        return
                    def w = word.toLowerCase()
                    if (!localDict.containsKey(w)) {
                        localDict.put(w, 1)
                    } else {
                        localDict.put(w, localDict.get(w) + 1)
                    }
                }
                localDict
            }.reduce { tdc, map1, map2 ->
                def map = new HashMap<String, Integer>()
                map1.each { Map.Entry<String, Integer> item ->
                    map.put(item.key, item.value)
                }
                map2.each { Map.Entry<String, Integer> item ->
                    if (map.containsKey(item.key))
                        map.put(item.key, map.get(item.key) + item.value)
                    else
                        map.put(item.key, item.value)
                }
                map
            }

            // Write results.
            StringBuilder sb = new StringBuilder()
            wordsRes = wordsRes.sort()
            wordsRes.each { k, v ->
                if (v >= 3)
                    sb.append("Word: " + k + " Occurrences: " + v + "\n")
            }
            new File("C:\\tmp\\ProcessFastSeminario\\onefile\\results.txt").text = sb.toString()
            long endTime = System.currentTimeMillis()
            tc.logManager.getLogger("test").info("Done! Execution time: ${endTime - startTime} milliseconds.")
        }

        .withNumInstances(1, 1)



        return ts
    }


    static def main(args) {
        GParsRuntime runtime = new GParsRuntime()
        runtime.numThreadsForDataParallelism = 2
        def ts = createMainTasksSet(runtime)
        runtime.run(ts)
    }

}