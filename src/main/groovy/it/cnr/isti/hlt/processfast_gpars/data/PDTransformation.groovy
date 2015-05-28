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

package it.cnr.isti.hlt.processfast_gpars.data

import groovy.transform.CompileStatic
import groovyx.gpars.GParsPool
import groovyx.gpars.GParsPoolUtil
import groovyx.gpars.ParallelEnhancer
import groovyx.gpars.pa.AbstractPAWrapper
import groovyx.gpars.pa.PAWrapper
import it.cnr.isti.hlt.processfast.data.CacheType
import it.cnr.isti.hlt.processfast.data.PDFunction
import it.cnr.isti.hlt.processfast.data.PDFunction2
import it.cnr.isti.hlt.processfast.data.PDFunctionCollector
import it.cnr.isti.hlt.processfast.data.PDPairFunction
import it.cnr.isti.hlt.processfast.utils.Pair
import it.cnr.isti.hlt.processfast_gpars.core.GParsTaskContext

import java.util.function.BiConsumer
import java.util.function.BinaryOperator
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Collector
import java.util.stream.Collectors
import java.util.stream.Stream


/**
 * Record handle for a base partitionable dataset transformation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
interface PDBaseTransformation {

    /**
     * Indicate if this transformation or real or is just some modification of parameters used to
     * customize how the partitionable dataset is computed.
     *
     * @return True if this is a real PD transformation, false otherwise.
     */
    boolean isRealTransformation()
}


@CompileStatic
class PDCustomizeTransformation implements PDBaseTransformation {

    /**
     * The closure customization code. The closure takes the owning partitionable dataset as argument.
     */
    Closure customizationCode = {}

    @Override
    boolean isRealTransformation() {
        return false
    }
}


@CompileStatic
class PDGparsParallelEnhancer {
    static public AbstractPAWrapper getParallel(Collection c) {
        GParsPoolUtil.getParallel(c)
    }

    static public AbstractPAWrapper getParallel(Map m) {
        GParsPoolUtil.getParallel(m)
    }


}

/**
 * Record handle for a real partitionable dataset transformation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
interface PDTransformation extends PDBaseTransformation {

    /**
     * Apply a specific transformation on source collection.
     *
     * @param source The source data collection.
     * @return The resulting collection after the operation has been applied.
     */
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source)

    /**
     * Indicate if the transformation need all available data or just some data chunk
     * to perform correctly.
     *
     * @return True if alla the data is necessary, false otherwise.
     */
    boolean needAllAvailableData()

    /**
     * Merge the results in "src" to the results in "dest".
     *
     * @param storageManager The storage manager to use.
     * @param src The source results.
     * @param dest The destination results or 'null' if there are no results in dest.
     * @param cacheType The type of cache where to merge results. Used when dest is 'null' and a new
     * structure for store intermediate results needs to be created.
     * @return The merged results.
     */
    public <T1> T1 mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T1 dest, CacheType cacheType)

    /**
     * Get the final results to be returned to action caller by
     * translating the specified final internal results.
     *
     * @param internalResults The computed final internal results.
     * @return The final results for action caller.
     */
    public <T1> PDResultsStorageIteratorProvider getFinalResults(T1 internalResults)
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#filter(it.cnr.isti.hlt.processfast.data.PDFunction)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDFilterTransformation<T> implements PDTransformation {

    final PDFunction<T, Boolean> code
    final GParsTaskContext tc
    final int maxBufferSize

    PDFilterTransformation(GParsTaskContext tc, PDFunction<T, Boolean> code, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (code == null)
            throw new NullPointerException("The programmer's code is 'null'")
        this.tc = tc
        this.code = code
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        def tdc = new GParsTaskDataContext(tc)
        source.filter { T item ->
            code.call(tdc, item)
        }
    }

    @Override
    boolean needAllAvailableData() {
        return false
    }

    @Override
    def <T> T mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T dest, CacheType cacheType) {
        PDResultsCollectionStorage storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsCollectionStorage) dest
        }

        storage.addResults((Collection<T>) src.collection)
        (T) storage
    }

    @Override
    def <T> PDResultsCollectionStorageIteratorProvider getFinalResults(T internalResults) {
        return new PDResultsCollectionStorageIteratorProvider((PDResultsCollectionStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#map(it.cnr.isti.hlt.processfast.data.PDFunction)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDMapTransformation<T, Out> implements PDTransformation {

    final PDFunction<T, Out> code
    final GParsTaskContext tc
    final int maxBufferSize

    PDMapTransformation(GParsTaskContext tc, PDFunction<T, Out> code, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (code == null)
            throw new NullPointerException("The programmer's code is 'null'")
        this.tc = tc
        this.code = code
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        def tdc = new GParsTaskDataContext(tc)
        source.map { T item ->
            code.call(tdc, item)
        }
    }

    @Override
    boolean needAllAvailableData() {
        return false
    }

    @Override
    def <T> T mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T dest, CacheType cacheType) {
        PDResultsCollectionStorage storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsCollectionStorage) dest
        }

        storage.addResults((Collection) src.collection)
        (T) storage
    }

    @Override
    def <T> PDResultsCollectionStorageIteratorProvider getFinalResults(T internalResults) {
        return new PDResultsCollectionStorageIteratorProvider((PDResultsCollectionStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PairPartitionableDataset#sortByKey(boolean)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDSortByKeyTransformation<K extends Comparable & Serializable, V extends Serializable> implements PDTransformation {

    final GParsTaskContext tc
    final int maxBufferSize
    final boolean sortAscending

    PDSortByKeyTransformation(GParsTaskContext tc, int maxBufferSize, boolean sortAscending) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")

        this.tc = tc
        this.maxBufferSize = maxBufferSize
        this.sortAscending = sortAscending
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        source.sort { Pair<K, V> item1, Pair<K, V> item2 ->
            return item1.v1.compareTo(item2.v1)
        }
    }

    @Override
    boolean needAllAvailableData() {
        return true
    }

    @Override
    def <T> T mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T dest, CacheType cacheType) {
        PDResultsSortedSetStorage storage
        if (dest == null) {
            storage = storageManager.createSortedSetStorage(storageManager.generateUniqueStorageID(), cacheType, sortAscending)
        } else {
            storage = (PDResultsSortedSetStorage) dest
        }

        def c = src.collection
        List toAdd = []
        Iterator<Pair<K, V>> iterator = c.iterator()
        while (iterator.hasNext()) {
            Pair<K, V> item = iterator.next()
            toAdd.add(new SortedSetItem<>(item.v1, item))
        }
        storage.addResults(toAdd)
        (T) storage
    }

    @Override
    def <T> PDResultsSortedSetStorageIteratorProvider getFinalResults(T internalResults) {
        return new PDResultsSortedSetStorageIteratorProvider((PDResultsSortedSetStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PairPartitionableDataset#reduceByKey(it.cnr.isti.hlt.processfast.data.PDFunction2)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDReduceByKeyTransformation<K extends Serializable, V extends Serializable> implements PDTransformation {

    final GParsTaskContext tc
    final int maxBufferSize
    final PDFunction2<V, V, V> code

    PDReduceByKeyTransformation(GParsTaskContext tc, PDFunction2<V, V, V> code, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (code == null)
            throw new NullPointerException("The programmer's code is 'null")

        this.tc = tc
        this.code = code
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        GParsTaskDataContext tdc = new GParsTaskDataContext(tc)

        Collection<Pair<K, V>> col = (Collection<Pair<K, V>>) source.collection
        Map<K, V> res = col.parallelStream().collect(Collectors.groupingBy(new Function<Pair<K, V>, K>() {

            K apply(Pair<K, V> item) {
                return item.getV1()
            }
        }, new PDFunctionCollector(tdc, code)));
        ArrayList<Pair<K, V>> values = new ArrayList<>()
        res.each { Map.Entry<K, V> node ->
            values.add(new Pair<K, V>(node.key, node.value))
        }
        PDGparsParallelEnhancer.getParallel(values)
    }


    @Override
    boolean needAllAvailableData() {
        return true
    }

    /*@Override
    @CompileStatic
    def <T> T mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T dest, CacheType cacheType) {
        PDResultsMapStorage<K, V> storage
        if (dest == null) {
            storage = storageManager.createMapStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsMapStorage<K, V>) dest
        }

        Collection<Pair<K, V>> c = (Collection) src.collection
        Iterator<Pair<K, V>> itColl = c.iterator()
        while (itColl.hasNext()) {
            Pair<K, V> item = itColl.next()
            V curVal = item.v2
            PDResultsCollectionStorage<V> listStorage = (PDResultsCollectionStorage<V>) storage.get(item.v1)
            if (listStorage.size() > 0) {
                V storeRes = (V) listStorage.getResults(0, 1).iterator().next()
                curVal = code.call(new GParsTaskDataContext(tc), storeRes, curVal)
                listStorage.clear()
            }
            ArrayList<V> res = new ArrayList<V>()
            res.add(curVal)
            listStorage.addResults(res);
        }
        (T) storage
    }*/

    @Override
    def <T> T mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T dest, CacheType cacheType) {
        PDResultsMapStorage<K, V> storage
        if (dest == null) {
            storage = storageManager.createMapStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsMapStorage) dest
        }

        Collection<Pair<K, V>> c = (Collection) src.collection

        Iterator<Pair<K, V>> it = c.iterator();
        while (it.hasNext()) {
            Pair<K, V> item = it.next();
            def curVal = item.v2
            V storedValue = storage.get(item.v1)
            if (storedValue != null) {
                curVal = code.call(new GParsTaskDataContext(tc), storedValue, curVal)
            }
            storage.put(item.v1, curVal)
        }
        /*GParsPoolUtil.eachParallel(c) { Pair<K, V> item ->
            def curVal = item.v2
            V storedValue = storage.get(item.v1)
            if (storedValue != null) {
                curVal = code.call(new GParsTaskDataContext(tc), storedValue, curVal)
            }
            storage.put(item.v1, curVal)
        }*/

        (T) storage
    }


    @Override
    def <T> PDResultsStorageIteratorProvider getFinalResults(T internalResults) {
        return new PDResultsMapStoragePairIteratorProvider((PDResultsMapStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#sort(boolean)}  operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDSortTransformation<K extends Comparable<K> & Serializable> implements PDTransformation {

    final GParsTaskContext tc
    final int maxBufferSize
    final boolean sortAscending

    PDSortTransformation(GParsTaskContext tc, int maxBufferSize, boolean sortAscending) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")

        this.tc = tc
        this.maxBufferSize = maxBufferSize
        this.sortAscending = sortAscending
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        source.sort { K item1, K item2 ->
            return item1.compareTo(item2)
        }
    }

    @Override
    boolean needAllAvailableData() {
        return true
    }

    @Override
    def <T> T mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T dest, CacheType cacheType) {
        PDResultsSortedSetStorage storage
        if (dest == null) {
            storage = storageManager.createSortedSetStorage(storageManager.generateUniqueStorageID(), cacheType, sortAscending)
        } else {
            storage = (PDResultsSortedSetStorage) dest
        }

        def c = src.collection
        def toAdd = []
        c.each { Pair<K, K> item ->
            toAdd.add(new SortedSetItem(item.v1, item))
        }
        storage.addResults(toAdd)
        (T) storage
    }

    @Override
    def <T> PDResultsSortedSetStorageIteratorProvider getFinalResults(T internalResults) {
        return new PDResultsSortedSetStorageIteratorProvider((PDResultsSortedSetStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#mapPair(it.cnr.isti.hlt.processfast.data.PDPairFunction)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDMapPairTransformation<T, K, V> implements PDTransformation {

    final PDPairFunction<T, K, V> code
    final GParsTaskContext tc
    final int maxBufferSize

    PDMapPairTransformation(GParsTaskContext tc, PDPairFunction<T, K, V> code, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (code == null)
            throw new NullPointerException("The programmer's code is 'null'")
        this.tc = tc
        this.code = code
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        def tdc = new GParsTaskDataContext(tc)
        source.map { T item ->
            code.call(tdc, item)
        }
    }

    @Override
    boolean needAllAvailableData() {
        return false
    }

    @Override
    def <Z> Z mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, Z dest, CacheType cacheType) {
        PDResultsCollectionStorage storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsCollectionStorage) dest
        }

        storage.addResults((Collection) src.collection)
        (Z) storage
    }

    @Override
    def <T> PDResultsCollectionStorageIteratorProvider getFinalResults(T internalResults) {
        return new PDResultsCollectionStorageIteratorProvider((PDResultsCollectionStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#groupBy(it.cnr.isti.hlt.processfast.data.PDFunction)}  operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDGroupByTransformation<T extends Serializable, K extends Serializable> implements PDTransformation {

    final PDFunction<T, K> code
    final GParsTaskContext tc
    final int maxBufferSize

    PDGroupByTransformation(GParsTaskContext tc, PDFunction<T, K> code, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (code == null)
            throw new NullPointerException("The programmer's code is 'null'")
        this.tc = tc
        this.code = code
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        def tdc = new GParsTaskDataContext(tc)
        PDGparsParallelEnhancer.getParallel(source.groupBy { T item ->
            code.call(tdc, item)
        })
    }

    @Override
    boolean needAllAvailableData() {
        return true
    }

    @Override
    def <T1> T1 mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T1 dest, CacheType cacheType) {
        PDResultsMapStorage<K, Collection<T>> storage
        if (dest == null) {
            storage = storageManager.createMapStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsMapStorage) dest
        }

        def c = src.collection
        c.each { Map.Entry<K, Collection> item ->
            Collection listStorage = storage.get(item.key)
            if (listStorage == null) {
                listStorage = new ArrayList<T>()
                storage.put(item.key, listStorage)
            }
            listStorage.addAll(item.value)
        }
        (T1) storage
    }

    @Override
    def <T1> PDResultsStorageIteratorProvider getFinalResults(T1 internalResults) {
        return new PDResultsMapStorageGroupByIteratorProvider((PDResultsMapStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#mapFlat(it.cnr.isti.hlt.processfast.data.PDFunction)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDMapFlatTransformation<T, Out> implements PDTransformation {

    final PDFunction<T, Iterator<Out>> code
    final GParsTaskContext tc
    final int maxBufferSize

    PDMapFlatTransformation(GParsTaskContext tc, PDFunction<T, Iterator<Out>> code, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (code == null)
            throw new NullPointerException("The programmer's code is 'null'")
        this.tc = tc
        this.code = code
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        def tdc = new GParsTaskDataContext(tc)
        def lret = GParsPoolUtil.collectManyParallel(source.collection) {
            item ->
                def res = code.call(tdc, item)
                List ret = []
                res.each {
                    ret.add(it)
                }
                ret
        }
        PDGparsParallelEnhancer.getParallel(lret)
    }

    @Override
    boolean needAllAvailableData() {
        return false
    }

    @Override
    def <T1> T1 mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T1 dest, CacheType cacheType) {
        PDResultsCollectionStorage storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsCollectionStorage) dest
        }

        storage.addResults((Collection) src.collection)
        (T1) storage
    }

    @Override
    def <T1> PDResultsCollectionStorageIteratorProvider getFinalResults(T1 internalResults) {
        return new PDResultsCollectionStorageIteratorProvider((PDResultsCollectionStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#mapFlat(it.cnr.isti.hlt.processfast.data.PDFunction)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDMapPairFlatTransformation<T extends Serializable, K extends Serializable, V extends Serializable> implements PDTransformation {

    final PDFunction<T, Iterator<Pair<K, V>>> code
    final GParsTaskContext tc
    final int maxBufferSize

    PDMapPairFlatTransformation(GParsTaskContext tc, PDFunction<T, Iterator<Pair<K, V>>> code, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (code == null)
            throw new NullPointerException("The programmer's code is 'null'")
        this.tc = tc
        this.code = code
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        def tdc = new GParsTaskDataContext(tc)
        Collection<T> coll = (Collection<T>) source.collection;
        Stream stream = coll.parallelStream().flatMap(new Function<T, Stream>() {

            Stream apply(T t) {
                Iterator<Pair<K, V>> res = code.call(tdc, t)
                ArrayList<Pair<K, V>> ret = new ArrayList<>()
                while (res.hasNext()) {
                    ret.add(res.next())
                }
                ret.parallelStream()
            }
        })

        List l = stream.collect(Collectors.toList());
        PDGparsParallelEnhancer.getParallel(l)

        /*PDGparsParallelEnhancer.getParallel(GParsPoolUtil.collectManyParallel(source.collection) {
            T item ->
                Iterator<Pair<K, V>> res = code.call(tdc, item)
                List<Pair<K, V>> ret = new ArrayList<Pair<K, V>>()
                res.each {
                    ret.add(it)
                }
                ret
        })*/

    }

    @Override
    boolean needAllAvailableData() {
        return false
    }

    @Override
    def <T1> T1 mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T1 dest, CacheType cacheType) {
        PDResultsCollectionStorage storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsCollectionStorage) dest
        }

        storage.addResults((Collection) src.collection)
        (T1) storage
    }


    @Override
    def <T1> PDResultsCollectionStorageIteratorProvider getFinalResults(T1 internalResults) {
        return new PDResultsCollectionStorageIteratorProvider((PDResultsCollectionStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#intersection(it.cnr.isti.hlt.processfast.data.PartitionableDataset)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDIntersectionTransformation<T extends Serializable> implements PDTransformation {

    final GParsTaskContext tc
    final GParsPartitionableDataset<T> toIntersect
    final int maxBufferSize

    PDIntersectionTransformation(GParsTaskContext tc, GParsPartitionableDataset<T> toIntersect, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (toIntersect == null)
            throw new NullPointerException("The partitionable dataset to intersect is 'null'")
        this.tc = tc
        this.toIntersect = toIntersect
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        Collection col = (Collection) source.filter { T item ->
            toIntersect.contains(item)
        }.collection
        PDGparsParallelEnhancer.getParallel(col.unique())
    }

    @Override
    boolean needAllAvailableData() {
        return true
    }

    @Override
    def <T1> T1 mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T1 dest, CacheType cacheType) {
        PDResultsCollectionStorage<T> storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsCollectionStorage) dest
        }

        def toAdd = []
        src.collection.each { T item ->
            if (!storage.contains(item))
                toAdd.add(item)
        }
        storage.addResults(toAdd)
        (T1) storage
    }

    @Override
    def <T1> PDResultsCollectionStorageIteratorProvider getFinalResults(T1 internalResults) {
        return new PDResultsCollectionStorageIteratorProvider((PDResultsCollectionStorage<T>) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#cartesian(it.cnr.isti.hlt.processfast.data.PartitionableDataset)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDCartesianTransformation<T extends Serializable> implements PDTransformation {

    final GParsTaskContext tc
    final GParsPartitionableDataset<T> toIntersect
    GParsPartitionableDataset<T> toIntersectCache
    long toIntersectSize = 0
    final int maxBufferSize

    PDCartesianTransformation(GParsTaskContext tc, GParsPartitionableDataset<T> toIntersect, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (toIntersect == null)
            throw new NullPointerException("The partitionable dataset to use in cartesian product is 'null'")
        this.tc = tc
        this.toIntersect = toIntersect
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        source
    }

    @Override
    boolean needAllAvailableData() {
        return true
    }

    @Override
    def <T1> T1 mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T1 dest, CacheType cacheType) {
        PDResultsCollectionStorage storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
            //toIntersectCache = (GParsPartitionableDataset) toIntersect.cache(CacheType.ON_DISK)
            toIntersectCache = (GParsPartitionableDataset) toIntersect
            toIntersectSize = toIntersectCache.count()
        } else {
            storage = (PDResultsCollectionStorage) dest
        }

        def toAdd = []
        src.collection.each { item ->
            addCartesianItems(storage, item, toIntersectCache, toIntersectSize, maxBufferSize)
        }
        (T1) storage
    }

    private
    def addCartesianItems(PDResultsCollectionStorage storage, item, GParsPartitionableDataset<T> pd, long pdSize, int maxBufferSize) {
        long curIdx = 0
        List l = []
        while (true) {
            long numItems = Math.min(maxBufferSize, pdSize - curIdx)
            if (numItems < 1)
                break
            def l2 = pd.take(curIdx, numItems)

            l2.each { item2 ->
                l.add(new Pair(item, item2))
            }
            storage.addResults(l)
            l.clear()
            curIdx += numItems
        }
    }


    @Override
    def <T1> PDResultsCollectionStorageIteratorProvider getFinalResults(T1 internalResults) {
        toIntersectCache.close()
        return new PDResultsCollectionStorageIteratorProvider((PDResultsCollectionStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#union(it.cnr.isti.hlt.processfast.data.PartitionableDataset)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDUnionTransformation<T extends Serializable> implements PDTransformation {

    final GParsTaskContext tc
    final GParsPartitionableDataset<T> toMerge
    final int maxBufferSize

    PDUnionTransformation(GParsTaskContext tc, GParsPartitionableDataset<T> toMerge, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (toMerge == null)
            throw new NullPointerException("The partitionable dataset to intersect is 'null'")
        this.tc = tc
        this.toMerge = toMerge
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        return source
    }

    @Override
    boolean needAllAvailableData() {
        return true
    }

    @Override
    def <T1> T1 mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T1 dest, CacheType cacheType) {
        PDResultsCollectionStorage storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsCollectionStorage) dest
        }

        List toAdd = []
        src.collection.each { item ->
            toAdd.add(item)
        }
        storage.addResults(toAdd)
        (T1) storage
    }

    @Override
    def <T1> PDResultsCollectionStorageIteratorProvider getFinalResults(T1 internalResults) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) internalResults
        boolean done = false
        long s = toMerge.count()
        long startIdx = 0
        while (!done) {
            long toRetrieve = Math.min(maxBufferSize, s - startIdx)
            if (toRetrieve == 0)
                break
            def items = toMerge.take(startIdx, toRetrieve)
            storage.addResults(items)
            startIdx += items.size()
        }

        return new PDResultsCollectionStorageIteratorProvider(storage, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#pair(it.cnr.isti.hlt.processfast.data.PartitionableDataset)} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDPairTransformation<T extends Serializable> implements PDTransformation {

    final GParsTaskContext tc
    final GParsPartitionableDataset<T> toPair
    final int maxBufferSize
    private long toPairSize

    PDPairTransformation(GParsTaskContext tc, GParsPartitionableDataset<T> toMerge, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (toMerge == null)
            throw new NullPointerException("The partitionable dataset to intersect is 'null'")
        this.tc = tc
        this.toPair = toMerge
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        return source
    }

    @Override
    boolean needAllAvailableData() {
        return true
    }

    @Override
    def <T1> T1 mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T1 dest, CacheType cacheType) {
        PDResultsCollectionStorage storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
            toPairSize = toPair.count()
        } else {
            storage = (PDResultsCollectionStorage) dest
        }

        List toAdd = []
        long curSize = storage.size()
        if (curSize >= toPairSize)
            return (T1) storage

        Collection curRes = (Collection) src.collection
        long maxItemsToAdd = toPairSize - curSize
        long itemsToAdd = Math.min(maxItemsToAdd, curRes.size())
        Collection otherDs = toPair.take(curSize, itemsToAdd)
        def otherDsIterator = otherDs.iterator()
        curRes.eachWithIndex { item, idx ->
            if (idx >= itemsToAdd)
                return
            toAdd.add(new Pair(item, otherDsIterator.next()))
        }
        storage.addResults(toAdd)
        (T1) storage
    }

    @Override
    def <T1> PDResultsCollectionStorageIteratorProvider getFinalResults(T1 internalResults) {
        PDResultsCollectionStorage storage = (PDResultsCollectionStorage) internalResults
        return new PDResultsCollectionStorageIteratorProvider(storage, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}

/**
 * A PD {@link it.cnr.isti.hlt.processfast.data.PartitionableDataset#distinct()} operation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 *
 */
@CompileStatic
class PDDistinctTransformation<T extends Serializable> implements PDTransformation {

    final GParsTaskContext tc
    final int maxBufferSize

    PDDistinctTransformation(GParsTaskContext tc, int maxBufferSize) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        this.tc = tc
        this.maxBufferSize = maxBufferSize
    }


    @Override
    AbstractPAWrapper applyTransformation(AbstractPAWrapper source) {
        Collection c = (Collection) source.collection
        PDGparsParallelEnhancer.getParallel(c.unique())
    }

    @Override
    boolean needAllAvailableData() {
        return true
    }

    @Override
    def <T1> T1 mergeResults(PDResultsStorageManager storageManager, AbstractPAWrapper src, T1 dest, CacheType cacheType) {
        PDResultsCollectionStorage<T> storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsCollectionStorage) dest
        }
        List toAdd = []
        src.collection.each { T item ->
            if (!storage.contains(item))
                toAdd.add(item)
        }
        storage.addResults(toAdd)
        return (T1) storage
    }

    @Override
    def <T1> PDResultsCollectionStorageIteratorProvider getFinalResults(T1 internalResults) {
        return new PDResultsCollectionStorageIteratorProvider((PDResultsCollectionStorage) internalResults, maxBufferSize)
    }

    @Override
    boolean isRealTransformation() {
        return true
    }
}