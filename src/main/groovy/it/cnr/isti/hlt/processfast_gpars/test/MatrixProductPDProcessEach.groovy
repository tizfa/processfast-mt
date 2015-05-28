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
import it.cnr.isti.hlt.processfast.core.TaskContext
import it.cnr.isti.hlt.processfast.core.TaskDataContext
import it.cnr.isti.hlt.processfast.data.CollectionDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.data.PartitionableDataset
import it.cnr.isti.hlt.processfast.utils.Pair
import it.cnr.isti.hlt.processfast_gpars.core.GParsRuntime


void initMatrices(double[][] matrix1, double[][] matrix2, int numRows1, int numCommon, int numCols2) {
    Random r = new Random()
    for (int i = 0; i < numRows1; i++) {
        for (int j = 0; j < numCommon; j++) {
            matrix1[i][j] = r.nextDouble()
        }
    }

    for (int i = 0; i < numCommon; i++) {
        for (int j = 0; j < numCols2; j++) {
            matrix2[i][j] = r.nextDouble()
        }
    }
}


@CompileStatic
def createProgram(GParsRuntime runtime) {

// Create main tasks set.
    def ts = runtime.createTaskSet()

// Define the task process.
    ts.task { TaskContext tc ->
        int numRows1 = 10000
        int numCommon = 100
        int numCols2 = 10000
        double[][] matrix1 = new double[numRows1][numCommon]
        double[][] matrix2 = new double[numCommon][numCols2]
        double[][] result = new double[numRows1][numCols2]

        // Initialize matrices.
        initMatrices(matrix1, matrix2, numRows1, numCommon, numCols2)

        tc.privateTaskDataDictionary.put("matrix1", matrix1)
        tc.privateTaskDataDictionary.put("matrix2", matrix2)
        tc.privateTaskDataDictionary.put("result", result)

        PartitionableDataset pd1 = tc.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider((0..numRows1 - 1)))
        long startTime = System.currentTimeMillis()
        pd1.enableLocalComputation(true).processEach { TaskDataContext tdc, Pair<Integer, Integer> item ->
            double[][] m1 = (double[][]) tdc.taskSharedData.dataDictionary.get("matrix1")
            double[][] m2 = (double[][]) tdc.taskSharedData.dataDictionary.get("matrix2")
            double[][] res = (double[][]) tdc.taskSharedData.dataDictionary.get("result")
            int i = item.v2
            for (int j = 0; j < m2[0].length; j++) {
                double val = 0
                for (int k = 0; k < m2.length; k++) {
                    val = val + (m1[i][k] * m2[k][j])
                }
                res[i][j] = val
            }
        }

        long endTime = System.currentTimeMillis()

        //tc.logManager.getLogger("Test").info("Result: ${result}")
        tc.logManager.getLogger("Test").info("Done! Computaton time: ${endTime - startTime} milliseconds.")
    }.withNumInstances(1, 1)
    ts
}


GParsRuntime runtime = new GParsRuntime()
runtime.numThreadsForDataParallelism = 10
def ts = createProgram(runtime)
runtime.run(ts)
