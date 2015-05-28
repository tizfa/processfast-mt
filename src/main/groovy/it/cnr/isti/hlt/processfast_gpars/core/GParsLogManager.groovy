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

package it.cnr.isti.hlt.processfast_gpars.core

import it.cnr.isti.hlt.processfast.core.LogLevel
import it.cnr.isti.hlt.processfast.core.LogManager
import it.cnr.isti.hlt.processfast.core.Logger
import it.cnr.isti.hlt.processfast_gpars.connector.GParsTaskLoadBalancingQueueConnector

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class GParsLogManager implements LogManager {

    final GParsProgramOrchestrator orchestrator

    /**
     * The set of declared loggers.
     */
    private def loggers = [:]

    GParsLogManager(GParsProgramOrchestrator orchestrator) {
        if (orchestrator == null)
            throw new NullPointerException("The orchestrator is 'null'")
        this.orchestrator = orchestrator
    }

    @Override
    Logger getLogger(String loggerName) {
        if (loggerName == null || loggerName.empty)
            throw new IllegalArgumentException("The logger name is 'null' or empty")
        if (loggers.containsKey(loggerName))
            return loggers.get(loggerName)

        loggers.put(loggerName, new GParsLogger(this, loggerName))

        return loggers.get(loggerName)
    }

    @Override
    void removeLogger(String loggerName) {
        if (loggerName == null || loggerName.empty)
            throw new IllegalArgumentException("The logger name is 'null' or empty")
        if (loggers.containsKey(loggerName))
            loggers.remove(loggerName)
    }

    /**
     * Stop processing data and terminate the logger processor.
     */
    void stopProcessing() {
        new GParsTaskLoadBalancingQueueConnector(orchestrator.logMessagesQueue).signalEndOfStream()
    }
}


class GParsLogger implements Logger {

    final GParsLogManager logManager
    final String loggerName
    private final GParsTaskLoadBalancingQueueConnector connector
    private LogLevel loggerLevel = LogLevel.INFO

    GParsLogger(GParsLogManager logManager, String loggerName) {
        if (logManager == null)
            throw new NullPointerException("The log manager is 'null'")
        if (loggerName == null || loggerName.empty)
            throw new IllegalArgumentException("The logger name is 'null' or empty")
        this.logManager = logManager
        this.loggerName = loggerName

        connector = new GParsTaskLoadBalancingQueueConnector(logManager.orchestrator.logMessagesQueue)
    }

    @Override
    void info(String msg) {
        if (msg == null)
            throw new NullPointerException("The msg is 'null'")

        LogMessage lm = new LogMessage(loggerName: this.loggerName, level: LogLevel.INFO, message: msg, throwable: null)
        connector.putValue(lm)
    }

    @Override
    void info(String msg, Throwable t) {
        if (msg == null)
            throw new NullPointerException("The msg is 'null'")
        if (t == null)
            throw new NullPointerException("The specified throwable is 'null'")

        LogMessage lm = new LogMessage(loggerName: this.loggerName, level: LogLevel.INFO, message: msg, throwable: t)
        connector.putValue(lm)
    }

    @Override
    void warning(String msg) {
        if (msg == null)
            throw new NullPointerException("The msg is 'null'")

        LogMessage lm = new LogMessage(loggerName: this.loggerName, level: LogLevel.WARNING, message: msg, throwable: null)
        connector.putValue(lm)
    }

    @Override
    void warning(String msg, Throwable t) {
        if (msg == null)
            throw new NullPointerException("The msg is 'null'")
        if (t == null)
            throw new NullPointerException("The specified throwable is 'null'")

        LogMessage lm = new LogMessage(loggerName: this.loggerName, level: LogLevel.WARNING, message: msg, throwable: t)
        connector.putValue(lm)
    }

    @Override
    void error(String msg) {
        if (msg == null)
            throw new NullPointerException("The msg is 'null'")

        LogMessage lm = new LogMessage(loggerName: this.loggerName, level: LogLevel.ERROR, message: msg, throwable: null)
        connector.putValue(lm)
    }

    @Override
    void error(String msg, Throwable t) {
        if (msg == null)
            throw new NullPointerException("The msg is 'null'")
        if (t == null)
            throw new NullPointerException("The specified throwable is 'null'")

        LogMessage lm = new LogMessage(loggerName: this.loggerName, level: LogLevel.ERROR, message: msg, throwable: t)
        connector.putValue(lm)
    }

    @Override
    void debug(String msg) {
        if (msg == null)
            throw new NullPointerException("The msg is 'null'")

        LogMessage lm = new LogMessage(loggerName: this.loggerName, level: LogLevel.DEBUG, message: msg, throwable: null)
        connector.putValue(lm)
    }

    @Override
    void debug(String msg, Throwable t) {
        if (msg == null)
            throw new NullPointerException("The msg is 'null'")
        if (t == null)
            throw new NullPointerException("The specified throwable is 'null'")

        LogMessage lm = new LogMessage(loggerName: this.loggerName, level: LogLevel.DEBUG, message: msg, throwable: t)
        connector.putValue(lm)
    }

    @Override
    void setLogLevel(LogLevel level) {
        if (level == null)
            throw new NullPointerException("The specified level is 'null'")
        LogMessage lm = new LogMessage(loggerName: this.loggerName, level: level, message: null, throwable: null)
        connector.putValue(lm)
        this.@loggerLevel = level
    }

    @Override
    LogLevel getLogLevel() {
        return this.@loggerLevel
    }
}