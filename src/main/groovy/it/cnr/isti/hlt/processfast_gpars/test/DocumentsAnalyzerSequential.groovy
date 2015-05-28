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
import it.cnr.isti.hlt.processfast_gpars.core.GParsRuntime

import java.util.regex.Pattern

@CompileStatic
class DocumentAnalyzerSequential {

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

        // Define the main process.
        ts.task { TaskContext tc ->
            long startTime = System.currentTimeMillis()
            List<String> fileList = []
            (0..10).each { mainDir ->
                (0..99).each {
                    def fname = "C:\\tmp\\ProcessFastSeminario\\multiplefiles\\${sprintf('%03d', mainDir)}\\wiki_${sprintf('%02d', it)}".toString()
                    if (new File(fname).exists())
                        fileList.add(fname)
                }
            }
            Map<String, Integer> mapWords = [:]
            def pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");
            int processed = 1
            fileList.each { String fname ->
                def articles = readArticles(fname)
                articles.eachWithIndex { article, idx ->
                    Map<String, Integer> localDict = [:]
                    def a = pattern.split(article)
                    a.each { String word ->
                        if (word.empty)
                            return
                        def w = word.toLowerCase()
                        if (!localDict.containsKey(w)) {
                            localDict.put(w, 1)
                        } else {
                            localDict.put(w, localDict.get(w) + 1)
                        }
                    }

                    localDict.each { String key, int value ->
                        if (key.length() > 3 && value >= 3) {
                            if (mapWords.containsKey(key)) {
                                mapWords.put(key, mapWords.get(key) + value)
                            } else {
                                mapWords.put(key, value)
                            }
                        }
                    }
                }

                processed++
                if (processed % 50 == 0)
                    tc.getLogManager().getLogger("test").info("Analyzed ${processed} files...")
            }

            // Write results.
            def sb = new StringBuilder()
            mapWords = mapWords.sort()
            mapWords.each { k, v ->
                sb.append("Word: " + k + " Occurrences: " + v + "\n")
            }
            new File("C:\\tmp\\ProcessFastSeminario\\multiplefiles\\results.txt").text = sb.toString()
            long endTime = System.currentTimeMillis()
            tc.getLogManager().getLogger("test").info("Done! Execution time: ${endTime - startTime} milliseconds.")
        }.withNumInstances(1, 1)

        return ts
    }

    static def main(args) {
        GParsRuntime runtime = new GParsRuntime()
        runtime.numThreadsForDataParallelism = 1
        def ts = createMainTasksSet(runtime)
        runtime.run(ts)
    }
}