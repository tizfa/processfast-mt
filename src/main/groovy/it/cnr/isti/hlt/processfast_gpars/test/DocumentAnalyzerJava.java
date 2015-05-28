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

package it.cnr.isti.hlt.processfast_gpars.test;

import it.cnr.isti.hlt.processfast.core.*;
import it.cnr.isti.hlt.processfast.data.CollectionDataSourceIteratorProvider;
import it.cnr.isti.hlt.processfast.data.PairPartitionableDataset;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_gpars.core.GParsRuntime;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by tiziano on 28/02/2015.
 */
public class DocumentAnalyzerJava {

    public static TaskDescriptor createTask(TaskSet ts) {
        return ts.task(tc -> {
            Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");
            List<String> articles = DocumentsAnalyzerWithPDIterations.readArticles("C:\\tmp\\ProcessFastSeminario\\wiki_00");
            int processed = 1;
            Map<String, Integer> mapWords = new HashMap<String, Integer>();
            double avgMaxLength = 0;
            for (int idx = 0; idx < articles.size(); idx++) {
                String article = articles.get(idx);
                String[] a = pattern.split(article);
                List<String> words = new ArrayList<String>();
                for (int j = 0; j < a.length; j++)
                    words.add(a[j]);
                PairPartitionableDataset<Integer, String> pd = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<String>(words));
                List<Pair<String, Integer>> results = (List<Pair<String, Integer>>) pd.mapPair((tdc, item) -> new Pair<String, Integer>(item.getV2().toLowerCase(), 1))
                        .reduceByKey((TaskDataContext tdc, Integer item1, Integer item2) -> item1 + item2)
                        .filter((tdc, item) -> item.getV2() >= 3 && item.getV1().length() > 3)
                        .collect();
                for (Pair<String, Integer> item : results) {
                    if (mapWords.containsKey(item.getV1())) {
                        mapWords.put(item.getV1(), mapWords.get(item.getV1()) + item.getV2());
                    } else {
                        mapWords.put(item.getV1(), item.getV2());
                    }
                }
                //pd.mapPair ( (tdc,  item) -> new Pair<String, Integer>(item.getV2().toLowerCase(), 1))
                //       .reduceByKey((tdc, item1, item2) -> item1 + item2).collect();

                //int maxLength = 2
                int maxLength = pd.map((tdc, item) -> item.getV2()).reduce(
                        (tdc, item1, item2) -> item1.length() > item2.length() ? item1 : item2).length();
                avgMaxLength = avgMaxLength + (maxLength - avgMaxLength) / processed;

                processed++;
                if (processed % 50 == 0)
                    tc.getLogManager().getLogger("test").info("Analyzed ${processed} documents...");
            }

            // Write results.
            StringBuilder sb = new StringBuilder();
            sb.append("Average max length: ${avgMaxLength}\n");
            Iterator<Map.Entry<String, Integer>> entries = mapWords.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<String, Integer> entry = entries.next();
                sb.append("Word: " + entry.getKey() + " Occurrences: " + entry.getValue() + "\n");
            }

            tc.getLogManager().getLogger("test").info("Done!");
        });

    }

    public static void main(String[] args) {
        GParsRuntime runtime = new GParsRuntime();
        runtime.setNumThreadsForDataParallelism(1);
        TaskSet ts = runtime.createTaskSet();
        createTask(ts);
        runtime.run(ts);
    }
}
