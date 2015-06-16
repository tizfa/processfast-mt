/*
 *
 * ****************
 * Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
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
 * ******************
 */

package it.cnr.isti.hlt.processfast_mt.core

import groovy.transform.CompileStatic
import it.cnr.isti.hlt.processfast.core.AtomicGetOperationsSet
import it.cnr.isti.hlt.processfast.core.AtomicOperationsSet
import it.cnr.isti.hlt.processfast.data.ReadableDictionary

import java.util.concurrent.Callable
import java.util.concurrent.locks.ReadWriteLock

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
@CompileStatic
class AtomicGetOperationCallable implements Callable<ReadableDictionary> {

    final String criticalSectionName;
    final ReadWriteLock lock;
    final ReadableDictionary inputData
    final AtomicGetOperationsSet operations
    final MTTaskContext tc;


    AtomicGetOperationCallable(String name, MTTaskContext tc, ReadWriteLock lock, ReadableDictionary inputData, AtomicGetOperationsSet operations) {
        this.criticalSectionName = name
        this.lock = lock;
        this.inputData = inputData;
        this.operations = operations;
        this.tc = tc;
    }

    @Override
    ReadableDictionary call() throws Exception {
        ReadableDictionary ret = null;
        lock.writeLock().lock()
        try {
            ret = operations.call(inputData, tc);
        } finally {
            lock.writeLock().unlock()
        }

        return ret
    }
}
