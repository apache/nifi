/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.logging;

/**
 * <p>
 * The ComponentLog provides a mechanism to ensure that all NiFi components are
 * logging and reporting information in a consistent way. When messages are
 * logged to the ComponentLog, each message has the following characteristics:
 * </p>
 *
 * <ul>
 * <li>
 * The <code>toString()</code> of the component is automatically prepended to
 * the message so that it is clear which component is providing the information.
 * This is important, since a single component may have many different instances
 * within the same NiFi instance.
 * </li>
 * <li>
 * If the last value in an Object... argument that is passed to the logger is a
 * Throwable, then the logged message will include a <code>toString()</code> of
 * the Throwable; in addition, if the component's logger is set to DEBUG level
 * via the logback configuration, the Stacktrace will also be logged. This
 * provides a mechanism to easily enable stacktraces in the logs when they are
 * desired without filling the logs with unneeded stack traces for messages that
 * end up occurring often.
 * </li>
 * <li>
 * Any message that is logged with a Severity level that meets or exceeds the
 * configured Bulletin Level for that component will also cause a Bulletin to be
 * generated, so that the message is visible in the UI, allowing Dataflow
 * Managers to understand that a problem exists and what the issue is.
 * </li>
 * </ul>
 */
public interface ComponentLog {

    void warn(String msg, Throwable t);

    void warn(String msg, Object... os);

    void warn(String msg);

    default void warn(LogMessage logMessage) {
        log(LogLevel.WARN, logMessage);
    }

    void trace(String msg, Throwable t);

    void trace(String msg, Object... os);

    void trace(String msg);

    default void trace(LogMessage logMessage) {
        log(LogLevel.TRACE, logMessage);
    }

    boolean isWarnEnabled();

    boolean isTraceEnabled();

    boolean isInfoEnabled();

    boolean isErrorEnabled();

    boolean isDebugEnabled();

    void info(String msg, Throwable t);

    void info(String msg, Object... os);

    void info(String msg);

    default void info(LogMessage logMessage) {
        log(LogLevel.INFO, logMessage);
    }

    String getName();

    void error(String msg, Throwable t);

    void error(String msg, Object... os);

    void error(String msg);

    default void error(LogMessage logMessage) {
        log(LogLevel.ERROR, logMessage);
    }

    void debug(String msg, Throwable t);

    void debug(String msg, Object... os);

    void debug(String msg);

    default void debug(LogMessage logMessage) {
        log(LogLevel.ERROR, logMessage);
    }

    default void log(LogLevel level, String msg, Throwable t) {
        switch (level) {
            case DEBUG:
                debug(msg, t);
                break;
            case ERROR:
            case FATAL:
                error(msg, t);
                break;
            case INFO:
                info(msg, t);
                break;
            case TRACE:
                trace(msg, t);
                break;
            case WARN:
                warn(msg, t);
                break;
        }
    }

    default void log(LogLevel level, String msg, Object... os) {
        switch (level) {
            case DEBUG:
                debug(msg, os);
                break;
            case ERROR:
            case FATAL:
                error(msg, os);
                break;
            case INFO:
                info(msg, os);
                break;
            case TRACE:
                trace(msg, os);
                break;
            case WARN:
                warn(msg, os);
                break;
        }
    }

    default void log(LogLevel level, String msg) {
        switch (level) {
            case DEBUG:
                debug(msg);
                break;
            case ERROR:
            case FATAL:
                error(msg);
                break;
            case INFO:
                info(msg);
                break;
            case TRACE:
                trace(msg);
                break;
            case WARN:
                warn(msg);
                break;
        }
    }

    default void log(LogMessage message) {
        switch (message.getLogLevel()) {
            case DEBUG:
                debug(message);
                break;
            case ERROR:
            case FATAL:
                error(message);
                break;
            case INFO:
                info(message);
                break;
            case TRACE:
                trace(message);
                break;
            case WARN:
                warn(message);
                break;
        }
    }

    default void log(LogLevel level, LogMessage logMessage) {
        String msg = logMessage.getMessage();
        Throwable t = logMessage.getThrowable();
        Object[] os = logMessage.getObjects();

        if (os != null && t != null) {
            Object[] ost = new Object[os.length + 1];
            System.arraycopy(os, 0, ost, 0, os.length);
            ost[ost.length - 1] = t;
            log(level, msg, ost);
        } else if (os != null) {
            log(level, msg, os);
        } else if (t != null) {
            log(level, msg, t);
        } else {
            log(level, msg);
        }
    }
}
