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

import groovyx.gpars.dataflow.DataflowBroadcast
import groovyx.gpars.dataflow.operator.DataflowEventAdapter
import groovyx.gpars.dataflow.operator.DataflowProcessor
import groovyx.gpars.group.DefaultPGroup
import groovyx.gpars.group.PGroup
import it.cnr.isti.hlt.processfast.connector.ConnectorType
import it.cnr.isti.hlt.processfast.core.LogLevel
import it.cnr.isti.hlt.processfast.core.Logger
import it.cnr.isti.hlt.processfast.data.RamDictionary
import it.cnr.isti.hlt.processfast.utils.Pair
import it.cnr.isti.hlt.processfast_mt.connector.MTBarrier
import it.cnr.isti.hlt.processfast_mt.connector.MTBroadcastQueueConnector
import it.cnr.isti.hlt.processfast_mt.connector.MTLoadBalancingQueueConnector
import it.cnr.isti.hlt.processfast_mt.connector.MTTaskLoadBalancingQueueConnector
import it.cnr.isti.hlt.processfast_mt.exception.MTTaskException

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * A program orchestrator based on GPars for a
 * processfast application.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class MTProgramOrchestrator {

    private final String SYSTEM_LOGGER = "PROCESSFAST_RUNTIME"

    /**
     * The used GPars runtime.
     */
    final MTProcessfastRuntime runtime

    /**
     * The global PGroup used to run the programmer's defined tasks.
     */
    PGroup tasksGroup

    /**
     * The queue used to log messages from tasks involved in the running
     * program.
     */
    MTLoadBalancingQueueConnector logMessagesQueue = new MTLoadBalancingQueueConnector(Integer.MAX_VALUE)

    /**
     * The queue used to signal exceptions from tasks involved in the running
     * program.
     */
    MTLoadBalancingQueueConnector exceptionMessaggesQueue = new MTLoadBalancingQueueConnector(Integer.MAX_VALUE)

    /**
     * The queue used to signal the start and the end of system tasks
     */
    DataflowBroadcast systemStartStopSignals = new DataflowBroadcast()

    /**
     * The queue used to signal the start and the end of tasks defined by program.
     */
    DataflowBroadcast startStopSignals = new DataflowBroadcast()

    /**
     * The set of running tasks sets.
     */
    final List<MTRunningTasksSet> runningTasksSets = []

    /**
     * The initial barrier used to synchronize all declared tasks before starting the program.
     */
    MTBarrier programStartBarrier

    /**
     * The initial barrier used to synchronize all system processors before starting the program.
     */
    MTBarrier systemProcessorsStartBarrier

    /**
     * The list of operators (tasks) declared in the running program.
     */
    private final runningOperators = []

    private DataflowProcessor loggerOperator
    private DataflowProcessor exceptionHandlerOperator

    MTLogManager internalLogManager

    /**
     * The fork-join pool used for data parallelism.
     */
    ForkJoinPool dataParallelismPool

    /**
     * The executor handling all operations related to mutual exclusions operations.
     */
    ExecutorService lockExecutor;

    private final HashMap<String, ReadWriteLock> atomicLocks;

    MTProgramOrchestrator(MTProcessfastRuntime runtime) {
        if (runtime == null)
            throw new NullPointerException("The specified runtime is 'null'")

        this.runtime = runtime
        internalLogManager = new MTLogManager(this)
        programStartBarrier = new MTBarrier("_program_start_barrier_")
        systemProcessorsStartBarrier = new MTBarrier("_system_processors_barrier_")
        atomicLocks = new HashMap<>()
    }


    synchronized ReadWriteLock getLock(String lockName) {
        if (lockName == null || lockName.empty)
            throw new IllegalArgumentException("The lock name is 'null' or empty")
        if (atomicLocks.containsKey(lockName)) {
            return atomicLocks.get(lockName)
        } else {
            atomicLocks.put(lockName, new ReentrantReadWriteLock())
            return atomicLocks.get(lockName);
        }

    }

    private void initGParsTasksGroup() {
        // Reset everything to initial state.
        logMessagesQueue = new MTLoadBalancingQueueConnector(Integer.MAX_VALUE)
        exceptionMessaggesQueue = new MTLoadBalancingQueueConnector(Integer.MAX_VALUE)
        systemStartStopSignals = new DataflowBroadcast()
        startStopSignals = new DataflowBroadcast()
        runningOperators.clear()
        loggerOperator = null
        exceptionHandlerOperator = null

        int threadPoolSize = computeNumDeclaredOperators() + 2
        tasksGroup = new DefaultPGroup(threadPoolSize)
        dataParallelismPool = new ForkJoinPool(runtime.numThreadsForDataParallelism)
        lockExecutor = Executors.newFixedThreadPool(2);
    }

    /**
     * Initialize the orchestrator and prepare the execution for
     * the specified tasks set.
     *
     * @param tasksSet The tasks set to be executed.
     */
    void run(MTTaskSet tasksSet) {

        // Declare all tasks contained in the tasks set.
        declareMainTasksSet(tasksSet)

        // Init tasks group.
        initGParsTasksGroup()

        // Create logger processor.
        loggerOperator = createLoggerProcessor()

        // Create exception handler processor.
        exceptionHandlerOperator = createExceptionHandlerProcessor()

        // Run system processors.
        startSystemProcessors()

        // Run initialization code.
        runInitializationCode()

        // Run program code.
        runProgram()

        // Run termination code.
        runTerminationCode()

        // Terminate system processors.
        terminateSystemProcessors()
    }


    private void startSystemProcessors() {
        systemProcessorsStartBarrier.initializeBarrier(3)
        systemStartStopSignals << true
        systemProcessorsStartBarrier.waitOnBarrier()
    }

    private void terminateSystemProcessors() {
        logMessagesQueue.signalEndOfStream()
        exceptionMessaggesQueue.signalEndOfStream()
        loggerOperator.join()
        exceptionHandlerOperator.join()
    }

    private int computeNumDeclaredOperators() {
        int numOperators = 0
        runningTasksSets.each { rts ->
            rts.tasksDeclared.each { name, task ->
                numOperators++
            }
        }
        numOperators
    }

    private void runProgram() {

        int numOperators = computeNumDeclaredOperators()
        programStartBarrier.initializeBarrier(numOperators)

        // Create one GPars operator for each task declared in the program.
        runningTasksSets.each { rts ->
            rts.tasksDeclared.each { name, task ->
                runningOperators << createTask(rts, task)
            }
        }

        // Run all the operators.
        startStopSignals << true

        // Wait all operators to complete.
        try {
            runningOperators*.join()
        } catch (e) {
            runningOperators*.terminate()
        }
    }


    private void runInitializationCode() {

        runningTasksSets.each { rt ->
            if (rt.tasksSetInitializationCode != null) {
                rt.tasksSetInitializationCode.call(new MTSystemContext(runtime))
            }
        }

    }


    private void runTerminationCode() {

        runningTasksSets.each { rt ->
            if (rt.tasksSetTerminationCode != null) {
                rt.tasksSetTerminationCode.call(new MTSystemContext(runtime))
            }
        }

    }


    private DataflowProcessor createTask(MTRunningTasksSet tasksSet, MTRunningTask task) {
        def op = null
        tasksGroup.with {
            def startStopChannel = startStopSignals.createReadChannel()
            def listener = new DataflowEventAdapter() {
                @Override
                boolean onException(final DataflowProcessor processor, final Throwable e) {
                    MTSystemContext sc = new MTSystemContext(runtime)
                    sc.logManager.getLogger(SYSTEM_LOGGER).error("Fatal error executing processfast task", new MTTaskException(task.virtualMachineName, task.taskName, "Executing task", e))
                    runningOperators*.terminate()
                    return true   //Indicate whether to terminate the operator or not
                }
            }
            op = operator(inputs: [startStopChannel], outputs: [], listeners: [listener]) {
                def tc
                if (!tasksSet.streamableTasksSet)
                    tc = new MTTaskContext(runtime, tasksSet, task)
                else
                    tc = new MTTaskContext(runtime, tasksSet, task)
                tc.logManager.getLogger(SYSTEM_LOGGER).debug("Task <${computeCompleteTaskName(task)}> started.")
                programStartBarrier.waitOnBarrier()
                task.taskCode.exec(tc)
                terminate()
                tc.logManager.getLogger(SYSTEM_LOGGER).debug("Task <${computeCompleteTaskName(task)}> terminated correctly.")
            }
        }

        return op
    }


    private String computeCompleteTaskName(MTRunningTask task) {
        def sb = new StringBuilder()
        sb.append(task.taskName)
        MTRunningTasksSet curTasksSet = task.ownerTasksSet
        while (curTasksSet != null) {
            sb.insert(0, curTasksSet.tasksSetName + "_")
            curTasksSet = curTasksSet.tasksSetParent
        }
        sb.toString()
    }


    private DataflowProcessor createLoggerProcessor() {
        def op = null
        runtime.logManager.getLogger(SYSTEM_LOGGER).setLogLevel(LogLevel.DEBUG)
        tasksGroup.with {
            def startStopChannel = systemStartStopSignals.createReadChannel()
            MTTaskLoadBalancingQueueConnector messages = new MTTaskLoadBalancingQueueConnector(logMessagesQueue)
            messages.taskName = "GlobalLoggerProcessor"

            op = operator([startStopChannel], []) {
                runtime.logManager.getLogger(SYSTEM_LOGGER).debug("Logger processor started.")
                systemProcessorsStartBarrier.waitOnBarrier()
                while (true) {
                    def msg = messages.getValue()
                    if (msg == null)
                        break
                    LogMessage lm = msg.payload
                    Logger logger = runtime.logManager.getLogger(lm.loggerName)
                    if (lm.isSettingDefaultLogLevel()) {
                        logger.setLogLevel(lm.level)
                        continue
                    }

                    switch (lm.level) {
                        case LogLevel.DEBUG:
                            lm.throwable != null ? logger.debug(lm.message, lm.throwable) : logger.debug(lm.message)
                            break
                        case LogLevel.INFO:
                            lm.throwable != null ? logger.info(lm.message, lm.throwable) : logger.info(lm.message)
                            break
                        case LogLevel.WARNING:
                            lm.throwable != null ? logger.warning(lm.message, lm.throwable) : logger.warning(lm.message)
                            break
                        case LogLevel.ERROR:
                            lm.throwable != null ? logger.error(lm.message, lm.throwable) : logger.error(lm.message)
                            break
                        default:
                            throw new IllegalArgumentException("The specified log level type is unknown: ${lm.level}")
                    }
                }

                terminate()
                runtime.logManager.getLogger(SYSTEM_LOGGER).debug("Logger processor terminated correctly.")
            }
        }

        return op
    }


    private DataflowProcessor createExceptionHandlerProcessor() {
        def op = null
        tasksGroup.with {
            def startStopChannel = systemStartStopSignals.createReadChannel()
            MTTaskLoadBalancingQueueConnector messages = new MTTaskLoadBalancingQueueConnector(exceptionMessaggesQueue)
            messages.taskName = "GlobalExceptionHandlerProcessor"
            op = operator([startStopChannel], []) {
                runtime.logManager.getLogger(SYSTEM_LOGGER).debug("Exception handler processor started.")
                systemProcessorsStartBarrier.waitOnBarrier()
                while (true) {
                    def msg = messages.getValue()
                    if (msg == null)
                        break
                    // TODO Implement this.
                }

                terminate()
                runtime.logManager.getLogger(SYSTEM_LOGGER).debug("Exception handler processor terminated correctly.")
            }
        }

        return op
    }

    /**
     * Declare the tasks set representing the application to be executed.
     *
     * @param tasksSet The tasks set.
     */
    private void declareMainTasksSet(MTTaskSet tasksSet) {
        if (tasksSet == null)
            throw new NullPointerException("The tasks set is 'null'")

        if (tasksSet.virtualConnectorsDeclared.size() > 0)
            throw new IllegalStateException("Inside the main tasks set, you can not declare any virtual connector")
        if (tasksSet.virtualBarriersDeclared.size() > 0)
            throw new IllegalStateException("Inside the main tasks set, you can not declare any virtual barrier")

        runningTasksSets.clear()
        MTRunningTasksSet rts = new MTRunningTasksSet()
        rts.streamableTasksSet = false
        rts.tasksSetName = "root_${System.currentTimeMillis()}"
        rts.tasksSetParent = null

        // Set initialization and termination code.
        rts.tasksSetInitializationCode = tasksSet.tasksSetInitializationCode
        rts.tasksSetTerminationCode = tasksSet.tasksSetTerminationCode

        rts.dataDictionary = tasksSet.dataDictionary

        // Create all connectors.
        createConnectorsOnTasksSet(tasksSet, rts)

        // Create all required barriers.
        createBarriersOnTasksSet(tasksSet, rts)

        // Create all required primitive tasks.
        createTasksOnTasksSet(tasksSet, rts)

        // Keep track of this tasks set.
        this.runningTasksSets << rts

        // Create all contained streamable tasks sets.
        tasksSet.tasksDeclared.each { taskDeclared ->
            if (taskDeclared instanceof MTTaskDescriptor)
                return

            MTTaskSetDescriptor td = taskDeclared
            declareStreamableTasksSet(rts, td)
        }

        // Initialize barriers.
        initializeBarriers(rts)
    }


    private void initializeBarriers(MTRunningTasksSet rts) {
        // Initialize barriers with right counters.
        rts.barriersCounter.each { String barrierName, int counter ->
            MTBarrier b = rts.barriers.get(barrierName)
            if (b == null)
                throw new IllegalArgumentException("The barrier with name ${barrierName} has not been declared!")
            b.initializeBarrier(counter)
        }
    }

    /**
     * Declare a running streamable tasks set in the orchestrator.
     *
     * @param tasksSetDescriptor The tasks set descriptor to declare.
     */
    private
    def declareStreamableTasksSet(MTRunningTasksSet parent, MTTaskSetDescriptor tasksSetDescriptor) {

        if (tasksSetDescriptor.withAttachedVirtualConnectorsCode == null)
            throw new IllegalStateException("A streamable tasks set must have at least 1 virtual connector declared!")

        for (int i = 0; i < tasksSetDescriptor.numInstances; i++) {
            MTRunningTasksSet rts = new MTRunningTasksSet()
            if (tasksSetDescriptor.withNameCode != null)
                rts.tasksSetName = tasksSetDescriptor.withNameCode.call(i)
            else
                rts.tasksSetName = "streamable_ts_${System.currentTimeMillis()}"

            rts.numTotalInstances = tasksSetDescriptor.numInstances
            rts.tasksSetParent = parent

            // Save tasks set data dictionary.
            if (tasksSetDescriptor.withDataDictionaryCode != null) {
                rts.dataDictionary = tasksSetDescriptor.withDataDictionaryCode.call(new WithDataDictionaryInfoImpl(taskName: rts.tasksSetName, numTotalInstances: rts.numTotalInstances, instanceNumber: i, dataDictionary: tasksSetDescriptor.taskInputDataDictionary))
            } else
                rts.dataDictionary = new RamDictionary()

            def tasksSet = tasksSetDescriptor.tasksSet

            // Keep track of virtual connectors.
            def virtualConnectors = tasksSetDescriptor.withAttachedVirtualConnectorsCode.call(new WithAttachedVirtualConnectorInfoImpl(taskName: rts.tasksSetName, numTotalInstances: rts.numTotalInstances, instanceNumber: i, dataDictionary: tasksSetDescriptor.taskInputDataDictionary))
            virtualConnectors.each { Pair<String, String> vc ->
                String virtualConnector = vc.v1
                String realConnector = vc.v2

                if (!tasksSet.virtualConnectorsDeclared.containsKey(virtualConnector))
                    throw new IllegalArgumentException("The virtual connector <${virtualConnector}> has not been created on tasks set")
                if (!rts.tasksSetParent.connectors.containsKey(realConnector))
                    throw new IllegalArgumentException("The real connector <${realConnector}> has not been created on parent tasks set")

                rts.virtualConnectors.put(virtualConnector, realConnector)
            }

            // Keep track of virtual barriers.
            if (tasksSetDescriptor.withAttachedVirtualBarriersCode != null) {
                def virtualBarriers = tasksSetDescriptor.withAttachedVirtualBarriersCode.call(new WithAttachedVirtualBarrierInfoImpl(taskName: rts.tasksSetName, numTotalInstances: rts.numTotalInstances, instanceNumber: i, dataDictionary: tasksSetDescriptor.taskInputDataDictionary))
                virtualBarriers.each { Pair<String, String> vc ->
                    String virtualBarrier = vc.v1
                    String realBarrier = vc.v2

                    if (!tasksSet.virtualBarriersDeclared.containsKey(virtualBarrier))
                        throw new IllegalArgumentException("The virtual barrier <${virtualBarrier}> has not been created on tasks set")
                    if (!rts.tasksSetParent.barriers.containsKey(realBarrier))
                        throw new IllegalArgumentException("The barrier <${realBarrier}> has not been declared in the tasks set <${rts.tasksSetParent.tasksSetName}>")

                    rts.virtualBarriers.put(virtualBarrier, realBarrier)
                }
            }

            // Set initialization and termination code.
            rts.tasksSetInitializationCode = tasksSet.tasksSetInitializationCode
            rts.tasksSetTerminationCode = tasksSet.tasksSetTerminationCode

            // Create all connectors.
            createConnectorsOnTasksSet(tasksSet, rts)

            // Create all required barriers.
            createBarriersOnTasksSet(tasksSet, rts)

            // Create all required primitive tasks.
            createTasksOnTasksSet(tasksSet, rts)

            // Keep track of this tasks set.
            this.runningTasksSets << rts

            // Create all contained streamable tasks sets.
            tasksSet.tasksDeclared.each { taskDeclared ->
                if (taskDeclared instanceof MTTaskDescriptor)
                    return

                MTTaskSetDescriptor td = taskDeclared
                declareStreamableTasksSet(rts, td)
            }

            // Initialize barriers.
            initializeBarriers(rts)
        }
    }

    void createTasksOnTasksSet(MTTaskSet tasksSet, MTRunningTasksSet runningTasksSet) {
        tasksSet.tasksDeclared.each { taskDeclared ->
            if (!(taskDeclared instanceof MTTaskDescriptor))
                return

            MTTaskDescriptor td = taskDeclared
            for (int i = 0; i < td.numInstances; i++) {
                MTRunningTask rt = new MTRunningTask(runningTasksSet, td.taskCode)

                // Set the instance numbers.
                rt.numInstance = i
                rt.numTotalInstances = td.numInstances

                // Assign name.
                if (td.withNameCode != null) {
                    String taskName = td.withNameCode.call(i)
                    rt.taskName = taskName
                }

                if (runningTasksSet.tasksDeclared.containsKey(rt.taskName))
                    throw new IllegalArgumentException("The task name ${rt.taskName} has been already declared on this tasks set. Each declared task must have an unique name!")

                // Save private data dictionary.
                if (td.withDataDictionaryCode != null) {
                    rt.privateDataDictionary = td.withDataDictionaryCode.call(new WithDataDictionaryInfoImpl(taskName: rt.taskName, numTotalInstances: td.numInstances, instanceNumber: i, dataDictionary: td.taskInputDataDictionary))
                } else {
                    rt.privateDataDictionary = new RamDictionary()
                }

                // Assign connectors.
                if (td.withConnectorsData != null) {
                    td.withConnectorsData.call(new WithConnectorInfoImpl(taskName: rt.taskName, numTotalInstances: td.numInstances, instanceNumber: i, connectorManager: runningTasksSet, dataDictionary: rt.privateDataDictionary))
                }

                // Assign barriers.
                if (td.withBarriersCode != null) {
                    def barriers = td.withBarriersCode.call(new WithBarrierInfoImpl(taskName: rt.taskName, numTotalInstances: td.numInstances, instanceNumber: i, dataDictionary: rt.privateDataDictionary))
                    barriers.each { barrierName ->
                        if (runningTasksSet.barriers.containsKey(barrierName)) {
                            // Operate on a real barrier.
                            int counter
                            if (!runningTasksSet.barriersCounter.containsKey(barrierName))
                                counter = 0
                            else
                                counter = runningTasksSet.barriersCounter.get(barrierName)
                            counter++
                            runningTasksSet.barriersCounter.put(barrierName, counter)
                            rt.barriersDeclared.add(barrierName)
                        } else if (runningTasksSet.virtualBarriers.containsKey(barrierName)) {
                            def realBarrier = runningTasksSet.virtualBarriers.get(barrierName)
                            // Operate on a virtual barrier.
                            int counter
                            if (!runningTasksSet.tasksSetParent.barriersCounter.containsKey(realBarrier))
                                counter = 0
                            else
                                counter = runningTasksSet.tasksSetParent.barriersCounter.get(realBarrier)
                            counter++
                            runningTasksSet.tasksSetParent.barriersCounter.put(realBarrier, counter)
                            rt.barriersDeclared.add(barrierName)
                        } else
                            throw new IllegalArgumentException("The barrier <${barrierName}> has not been declared or, if virtual, has noot been attached to a real barrier!")
                    }
                }

                // Save the task on tasks set.
                runningTasksSet.tasksDeclared.put(rt.taskName, rt)
            }


        }
    }

    void createBarriersOnTasksSet(MTTaskSet tasksSet, MTRunningTasksSet runningTasksSet) {
        tasksSet.barriersDeclared.each { String name, unused ->
            runningTasksSet.barriers.put(name, new MTBarrier(name))
        }
    }

    private void createConnectorsOnTasksSet(MTTaskSet tasksSet, MTRunningTasksSet runningTasksSet) {
        // Create all connectors.
        tasksSet.connectorsDeclared.each { String connectorName, ConnectorType connectorType ->
            int connectorSize = tasksSet.connectorsSizeDeclared.get(connectorName)
            switch (connectorType) {
                case ConnectorType.LOAD_BALANCING_QUEUE:
                    runningTasksSet.connectors.put(connectorName, new ConnectorInfo(connector: new MTLoadBalancingQueueConnector(connectorSize)))
                    break
                case ConnectorType.BROADCAST_QUEUE:
                    runningTasksSet.connectors.put(connectorName, new ConnectorInfo(connector: new MTBroadcastQueueConnector(connectorSize)))
                    break
                default:
                    throw new IllegalArgumentException("The specified connector type is unknown: ${connectorType}")
            }
        }
    }
}


