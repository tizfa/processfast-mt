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

package it.cnr.isti.hlt.processfast_gpars.connector

import java.util.concurrent.CyclicBarrier

/**
 * A barrier used to synchronize multiple threads on a specific execution point.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class GParsBarrier {

    /**
     * The barrier name.
     */
    final String name


    int numTotalActors = -1
    int currentWaiting = 0


    GParsBarrier(String name) {
        if (name == null || name.empty)
            throw new IllegalArgumentException("The specified name is 'null' or empty")
        this.@name = name
    }

    /**
     * Initialize the barrier object specifying the number of actors that must synchronize on
     * this object.
     *
     * @param numSynchronizedActors The number of actors involved in the barrier.
     */
    synchronized void initializeBarrier(int numSynchronizedActors) {
        if (numSynchronizedActors < 1)
            throw new IllegalArgumentException("The number of threads that synchronize must be greater-equals than 1. Current value: ${numSynchronizedActors}")
        numTotalActors = numSynchronizedActors
        currentWaiting = 0
    }

    /**
     * Indicate if the barrier has been or not initialized.
     *
     * @return True if the barrier has been initialized, false otherwise.
     */
    synchronized boolean isInitialized() {
        numTotalActors != -1
    }

    /**
     * Reset the barrier to its initial state.
     */
    synchronized void reset() {
        if (!isInitialized())
            throw new IllegalStateException("The barrier is not initialized")
        currentWaiting = 0
    }

    /**
     * Synchronize the caller on the barrier. The method will nor return until all
     * actors involved have not joined the barrier.
     */
    synchronized void waitOnBarrier() {
        currentWaiting++
        if (currentWaiting < numTotalActors)
            wait()
        else {
            notifyAll()
            reset()
        }
    }

    /**
     * Get the total number of actors required to join the barrier.
     *
     * @return The total number of actors required to join the barrier.
     */
    synchronized int numActorsInvolved() {
        numTotalActors
    }
}
