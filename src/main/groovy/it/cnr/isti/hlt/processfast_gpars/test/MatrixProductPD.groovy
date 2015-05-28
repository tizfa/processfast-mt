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
import it.cnr.isti.hlt.processfast.data.PartitionableDataset
import it.cnr.isti.hlt.processfast.data.RamDoubleMatrixIteratorProvider
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
        double[][] result = new double[numRows1][]

        // Initialize matrices.
        initMatrices(matrix1, matrix2, numRows1, numCommon, numCols2)

        tc.privateTaskDataDictionary.put("matrix2", matrix2)

        PartitionableDataset<Pair<Integer, double[]>> pd1 = tc.createPairPartitionableDataset(new RamDoubleMatrixIteratorProvider(matrix1, true));
        long startTime = System.currentTimeMillis();
        def results = pd1.enableLocalComputation(true).map { TaskDataContext tdc, Pair<Integer, double[]> item ->
            double[] m1Row = item.v2
            int i = item.v1
            double[][] m2 = (double[][]) tdc.taskSharedData.dataDictionary.get("matrix2")
            double[] products = new double[m2[0].length]
            for (int j = 0; j < m2[0].length; j++) {
                double val = 0
                for (int k = 0; k < m2.length; k++) {
                    val = val + (m1Row[k] * m2[k][j])
                }
                products[j] = val
            }
            new Pair<Integer, double[]>(i, products)
        }.collect()
        results.each { Pair<Integer, double[]> item ->
            int i = item.v1
            double[] products = item.v2
            result[i] = products
        }

        long endTime = System.currentTimeMillis()

        //tc.logManager.getLogger("Test").info("Result: ${result}")
        tc.logManager.getLogger("Test").info("Done! Computaton time: ${endTime - startTime} milliseconds.")
    }

    .withNumInstances(1, 1)
    ts
}


GParsRuntime runtime = new GParsRuntime()
runtime.numThreadsForDataParallelism = 8
def ts = createProgram(runtime)
runtime.run(ts)
