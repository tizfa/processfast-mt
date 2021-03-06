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

import it.cnr.isti.hlt.processfast.core.LogLevel

/**
 * An internal log message record exchanged with logger operator.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class LogMessage implements Serializable {

    /**
     * The logger name.
     */
    String loggerName

    /**
     * The level of message.
     */
    LogLevel level

    /**
     * The message to log.
     */
    String message

    /**
     * The optional throwable to log.
     */
    Throwable throwable

    /**
     * Indicate if this message is used to set the default log level. If true, only {@link #loggerName} and
     * {@link #level} properties will be used.
     */
    boolean settingDefaultLogLevel = false
}
