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

import it.cnr.isti.hlt.processfast.data.CollectionDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast_gpars.core.GParsRuntime

GParsRuntime runtime = new GParsRuntime()
runtime.numThreadsForDataParallelism = 10
def ts = runtime.createTaskSet()

/*ts.task { tc ->

    Random r = new Random()
    def l = []
    (1..10000000).each {
        l << r.nextInt(100)
    }
    //l << 2 << 13 << 4 << 2 << 7 << 9 << 10 << 12 << 10 << 15 << 24 << 27 << 3 << 8

    def pd = tc.createPartitionableDataset(new CollectionDataSourceIteratorProvider<Integer>(l))
    long found = pd.filter{ tdc, item ->
        item.v2 % 2 == 0
    }.count()
    tc.logManager.getLogger("Test").info("Found ${found} items!")

    def pdRes = pd.filter { tdc, item ->
        item.v2 % 2 == 0
    }.map { tdc, item ->
        item.v2
    }

    def items = pdRes.collect().subList(0, 100)
    tc.logManager.getLogger("Test").info("Computed results: ${items}")
    def itemsDistinct = pdRes.distinct().map { tdc, it -> it*it}.collect()
    tc.logManager.getLogger("Test").info("Computed results with distinct: num items ${itemsDistinct.size()}")
    tc.logManager.getLogger("Test").info("Computed results with distinct: ${itemsDistinct}")
}*/

ts.task { tc ->
    Random r = new Random()
    def l = [12, 5, 8, 9, 23, 23, 23, 24, 2, 5, 11]
    //def l = [1,2,3]
    def l2 = [2, 8, 5, 7, 9, 5]
    def pd = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<Integer>(l))
    def pd2 = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<Integer>(l2))

    def results = pd.withPartitionSize(3).pair(pd2).map { tdc, item -> [item.v1.v2, item.v2.v2] }.collect()
    tc.logManager.getLogger("Test").info("Results items: ${results}")

    /*pd = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<Integer>(l))
    pd2 = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<Integer>(l2))
    while (true) {
        pd = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<Integer>(l))
        def results = pd.map { tdc, item -> item.v2 }.withPartitionSize(3).map { tdc, item -> item * 2 }.take(2, 4)
        //tc.logManager.getLogger("Test").info("Results size: ${results.size()}")
        //tc.logManager.getLogger("Test").info("Results items: ${results}")
    }

    def cachedResults = pd.map{tdc, item ->item.v2}
            .intersection(pd2.map{tdc,item->item.v2})
            .mapFlat{ tdc, item ->
        ["Ratio: "+item/2, "Product: "+item*2]
    }.cache(CacheType.RAM)
    tc.logManager.getLogger("Test").info("Results size: ${cachedResults.count()}, Results: ${cachedResults.collect()}")*/

    /*results = pd.map{tdc, item ->item.v2}
            .union(pd2.map{tdc,item->item.v2})
            .collect()
    tc.logManager.getLogger("Test").info("Results size: ${results.size()}")
    tc.logManager.getLogger("Test").info("Results items: ${results}")

    def sum = pd2.map{tdc, item -> item.v2}.reduce{tdc, it1, it2->it1+it2}
    tc.logManager.getLogger("Test").info("The sum of ${l2} is: ${sum}")


    def ret = pd.map{tdc, item->item.v2}.groupBy{tdc, item ->
        if (item <= 10)
            return "g1"
        else
            return "g2"
    }.collect()
    ret.each { item ->
        def items = item.v2.iterator()
        def sb = new StringBuilder()
        while (items.hasNext()) {
            sb.append(" "+items.next())
        }
        tc.logManager.getLogger("Test").info("Key: ${item.v1} Items: ${sb.toString()}")
    }

    ret = pd.map{tdc, item->item.v2}.mapPair{tdc, item ->new Pair(item, (item**2)-1)}
            .groupBy{tdc, item ->item.v1}
            .collect()
    ret.each { item ->
        def items = item.v2.iterator()
        def sb = new StringBuilder()
        while (items.hasNext()) {
            sb.append(" "+items.next())
        }
        tc.logManager.getLogger("Test").info("Key: ${item.v1} Items: ${sb.toString()}")
    }*/

    //ret = pd2.map{tdc,item->item.v2}.cartesian(pd.map{tdc,item->item.v2}).collect()
    //tc.logManager.getLogger("Test").info("Cartesian is: ${ret}")

    //ret = pd.sort(true).collect()
    //tc.logManager.getLogger("Test").info("Sort by key results is: ${ret}")

    /*def l3 = ["Now", "yes", "now", "yes", "house", "House", "dog", "cat", "nOw"]
    def pd3 = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<String>(l3))
    ret = pd3.mapPair{tdc, item ->
        new Pair(item.v2.toLowerCase(), 1)
    }.reduceByKey{tdc, v1, v2 ->v1+v2}.collect()
    tc.logManager.getLogger("Test").info("Reduce by key: ${ret}")

    def pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])")
    String text = "Sopra la panca la capra campa, sotto la panca la capra crepa"
    def a = pattern.split(text)
    PairPartitionableDataset<Integer, String> pd4 = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<String>(a.toList()))
    def results2 = pd4.mapPair { tdc, item -> new Pair(item.v2, 1) }
            .reduceByKey { tdc, item1, item2 -> item1 + item2 }
            .filter { tdc, item -> item.v2 >= 0 }
            .collect()
    tc.logManager.getLogger("Test").info("Res: ${results2}")*/

}

runtime.run(ts)
