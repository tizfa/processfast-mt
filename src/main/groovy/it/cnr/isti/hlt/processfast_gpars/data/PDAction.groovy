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
import groovyx.gpars.pa.AbstractPAWrapper
import it.cnr.isti.hlt.processfast.data.CacheType
import it.cnr.isti.hlt.processfast.data.PDFunction2
import it.cnr.isti.hlt.processfast.data.PDProcedure
import it.cnr.isti.hlt.processfast_gpars.core.GParsTaskContext

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
@CompileStatic
interface PDAction<Out> {

    /**
     * Apply the action on a source collection.
     *
     * @param source
     * @return
     */
    Out applyAction(AbstractPAWrapper source)

    /**
     * Merge the results in "src" to the results in "dest".
     *
     * @param storageManager The storage manager available.
     * @param src The source results.
     * @param dest The destination results or 'null' if there are no results in dest.
     * @param cacheType The type of cache where to merge results. Used when dest is 'null' and a new
     * structure for store intermediate results needs to be created.
     * @return The merged results.
     */
    public <T> T mergeResults(PDResultsStorageManager storageManager, Out src, T dest, CacheType cacheType)

    /**
     * Indicate if the action need more results to compute the
     * desired behaviour.
     *
     * @param currentResults The current results.
     * @return True if the action need more results, false otherwise.
     */
    public <T> boolean needMoreResults(T currentResults)

    /**
     * Get the final results to be returned to action caller by
     * translating the specified final internal results.
     *
     * @param internalResults The computed final internal results.
     * @return The final results for action caller.
     */
    public <T> Out getFinalResults(PDResultsStorageManager storageManager, T internalResults)
}

/**
 * A Gpars PD action for {@link GParsPartitionableDataset#count()} method.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
@CompileStatic
class PDCountAction implements PDAction<Long> {


    @Override
    Long applyAction(AbstractPAWrapper source) {
        return source.size()
    }


    @Override
    def <T> Long getFinalResults(PDResultsStorageManager storageManager, T internalResults) {
        return internalResults as Long
    }

    @Override
    <T> T mergeResults(PDResultsStorageManager storageManager, Long src, T dest, CacheType cacheType) {
        if (dest == null)
            return (T) src
        else
            return (T) (long) src + (dest as Long)
    }

    @Override
    def <T> boolean needMoreResults(T currentResults) {
        return true
    }
}


@CompileStatic
class PDContainsAction<ItemType extends Serializable> implements PDAction<Boolean> {

    final GParsTaskContext tc
    final ItemType item

    PDContainsAction(GParsTaskContext tc, ItemType item) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (item == null)
            throw new NullPointerException("The item is 'null'")
        this.tc = tc
        this.item = item
    }


    @Override
    Boolean applyAction(AbstractPAWrapper source) {
        Collection c = (Collection) source.filter { i ->
            i.equals(item)
        }.collection
        c.size() >= 1
    }


    @Override
    def <T> Boolean getFinalResults(PDResultsStorageManager storageManager, T internalResults) {
        return internalResults
    }

    @Override
    <T> T mergeResults(PDResultsStorageManager storageManager, Boolean src, T dest, CacheType cacheType) {
        if (dest == null)
            return (T) src
        else
            return (T) (src || dest)
    }

    @Override
    def <T> boolean needMoreResults(T currentResults) {
        boolean containsItem = currentResults
        return !containsItem
    }
}

@CompileStatic
class PDCollectAction<Out extends Serializable> implements PDAction<Collection<Out>> {

    final GParsTaskContext tc

    PDCollectAction(GParsTaskContext tc) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        this.tc = tc
    }

    @Override
    Collection<Out> applyAction(AbstractPAWrapper source) {
        return (Collection) source.collection
    }


    @Override
    def <T> Collection<Out> getFinalResults(PDResultsStorageManager storageManager, T internalResults) {
        PDResultsCollectionStorage<Out> results = (PDResultsCollectionStorage<Out>) internalResults
        if (results.size() == 0)
            return []
        def res = results.getResults(0, results.size())
        storageManager.deleteCollectionStorage(results.storageID)
        res
    }

    @Override
    <T> T mergeResults(PDResultsStorageManager storageManager, Collection<Out> src, T dest, CacheType cacheType) {
        if (dest == null) {
            PDResultsCollectionStorage<Out> storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
            storage.addResults(src)
            return (T) storage
        } else {
            PDResultsCollectionStorage<Out> results = (PDResultsCollectionStorage<Out>) dest
            results.addResults(src)
            return (T) results
        }
    }

    @Override
    def <T> boolean needMoreResults(T currentResults) {
        return true
    }
}


@CompileStatic
class PDProcessAction<Out extends Serializable> implements PDAction<Void> {

    final GParsTaskContext tc
    final PDProcedure<Out> func

    PDProcessAction(GParsTaskContext tc, PDProcedure<Out> func) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (func == null)
            throw new NullPointerException("The func is 'null'")
        this.tc = tc
        this.func = func
    }

    @Override
    Void applyAction(AbstractPAWrapper source) {
        GParsTaskDataContext tdc = new GParsTaskDataContext(tc)
        source.filter { Out item ->
            func.call(tdc, (Out) item)
            false
        }.collection as Void
    }


    @Override
    def <T> Void getFinalResults(PDResultsStorageManager storageManager, T internalResults) {

    }

    @Override
    <T> T mergeResults(PDResultsStorageManager storageManager, Void src, T dest, CacheType cacheType) {
        return null
    }

    @Override
    def <T> boolean needMoreResults(T currentResults) {
        return true
    }
}


@CompileStatic
class PDReduceAction<Out extends Serializable> implements PDAction<Out> {

    final GParsTaskContext tc
    final PDFunction2<Out, Out, Out> code

    PDReduceAction(GParsTaskContext tc, PDFunction2<Out, Out, Out> code) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (code == null)
            throw new NullPointerException("The programmer's code is ' null'")
        this.tc = tc
        this.code = code
    }

    @Override
    Out applyAction(AbstractPAWrapper source) {
        GParsTaskDataContext tdc = new GParsTaskDataContext(tc)
        return (Out) source.reduce { Out item1, Out item2 ->
            code.call(tdc, item1, item2)
        }
    }


    @Override
    def <T> Out getFinalResults(PDResultsStorageManager storageManager, T internalResults) {
        (Out) internalResults
    }

    @Override
    <T> T mergeResults(PDResultsStorageManager storageManager, Out src, T dest, CacheType cacheType) {
        if (dest == null)
            return (T) src
        else
            return (T) code.call(new GParsTaskDataContext(tc), (Out) src, (Out) dest)
    }

    @Override
    def <T> boolean needMoreResults(T currentResults) {
        return true
    }
}


@CompileStatic
class PDTakeAction<Out extends Serializable> implements PDAction<Collection<Out>> {

    final GParsTaskContext tc
    final long startFrom
    final long numItems

    PDTakeAction(GParsTaskContext tc, long startFrom, long numItems) {
        if (tc == null)
            throw new NullPointerException("The task context is 'null'")
        if (startFrom < 0)
            throw new IllegalArgumentException("The startFrom parameter value is invalid: ${startFrom}")
        if (numItems < 1)
            throw new IllegalArgumentException("The numItems parameter value is invalid: ${numItems}")

        this.tc = tc
        this.startFrom = startFrom
        this.numItems = numItems
    }

    @Override
    Collection<Out> applyAction(AbstractPAWrapper source) {
        return (Collection) source.collection
    }


    @Override
    def <T> Collection<Out> getFinalResults(PDResultsStorageManager storageManager, T internalResults) {
        PDResultsCollectionStorage<Out> results = (PDResultsCollectionStorage<Out>) internalResults
        long s = results.size()
        if (s == 0)
            return []
        if (startFrom >= s)
            throw new IllegalArgumentException("The requested startFrom value is greater than available results size: startFrom [${startFrom}] >= results size ${s}")
        long endIdx = startFrom + numItems
        if (endIdx > s)
            throw new IllegalArgumentException("The requested numItems value is greater than available results size: numItems [${numItems}] >= available items ${s - startFrom}")

        def res = results.getResults(startFrom, startFrom + numItems)
        storageManager.deleteCollectionStorage(results.storageID)
        res
    }

    @Override
    <T> T mergeResults(PDResultsStorageManager storageManager, Collection<Out> src, T dest, CacheType cacheType) {
        PDResultsCollectionStorage<Out> storage
        if (dest == null) {
            storage = storageManager.createCollectionStorage(storageManager.generateUniqueStorageID(), cacheType)
        } else {
            storage = (PDResultsCollectionStorage<Out>) dest
        }
        storage.addResults(src)
        return (T) storage
    }

    @Override
    def <T> boolean needMoreResults(T currentResults) {
        return true
    }
}