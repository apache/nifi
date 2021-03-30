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
package org.apache.nifi.mock;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stubs out the functionality of a ComponentLog so that it can
 * be used during initialization of a component.
 *
 */
public class MockComponentLogger implements ComponentLog {

    private static final Logger logger = LoggerFactory
            .getLogger(MockComponentLogger.class);

    @Override
    public void warn(String msg, Throwable t) {
        logger.warn(msg, t);
    }

    @Override
    public void warn(String msg, Object[] os) {
        logger.warn(msg, os);
    }

    @Override
    public void warn(String msg, Object[] os, Throwable t) {
        logger.warn(msg, os);
        logger.warn("", t);
    }

    @Override
    public void warn(String msg) {
        logger.warn(msg);
    }

    @Override
    public void warn(LogMessage logMessage) {
        logger.warn(logMessage.getMessage(), logMessage.getObjects());
        logger.warn("", logMessage.getThrowable());
    }

    @Override
    public void trace(String msg, Throwable t) {
        logger.trace(msg, t);
    }

    @Override
    public void trace(String msg, Object[] os) {
        logger.trace(msg, os);
    }

    @Override
    public void trace(String msg) {
        logger.trace(msg);
    }

    @Override
    public void trace(String msg, Object[] os, Throwable t) {
        logger.trace(msg, os);
        logger.trace("", t);
    }

    @Override
    public void trace(LogMessage logMessage) {
        logger.trace(logMessage.getMessage(), logMessage.getObjects());
        logger.trace("", logMessage.getThrowable());
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public void info(String msg, Throwable t) {
        logger.info(msg, t);
    }

    @Override
    public void info(String msg, Object[] os) {
        logger.info(msg, os);
    }

    @Override
    public void info(String msg) {
        logger.info(msg);

    }

    @Override
    public void info(String msg, Object[] os, Throwable t) {
        logger.trace(msg, os);
        logger.trace("", t);
    }

    @Override
    public void info(LogMessage logMessage) {
        String msg = logMessage.getMessage();
        Throwable t = logMessage.getThrowable();
        Object[] os = logMessage.getObjects();

        if (os != null && t != null) {
            info(msg, os, t);
        } else if (os != null) {
            info(msg, os);
        } else if (t != null) {
            info(msg, t);
        } else {
            info(msg);
        }
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public void error(String msg, Throwable t) {
        logger.error(msg, t);
    }

    @Override
    public void error(String msg, Object[] os) {
        logger.error(msg, os);
    }

    @Override
    public void error(String msg) {
        logger.error(msg);
    }

    @Override
    public void error(String msg, Object[] os, Throwable t) {
        logger.error(msg, os);
        logger.error("", t);
    }

    @Override
    public void error(LogMessage logMessage) {
        String msg = logMessage.getMessage();
        Throwable t = logMessage.getThrowable();
        Object[] os = logMessage.getObjects();

        if (os != null && t != null) {
            error(msg, os, t);
        } else if (os != null) {
            error(msg, os);
        } else if (t != null) {
            error(msg, t);
        } else {
            error(msg);
        }
    }

    @Override
    public void debug(String msg, Throwable t) {
        logger.debug(msg, t);
    }

    @Override
    public void debug(String msg, Object[] os) {
        logger.debug(msg, os);
    }

    @Override
    public void debug(String msg, Object[] os, Throwable t) {
        logger.debug(msg, os);
        logger.debug("", t);
    }

    @Override
    public void debug(String msg) {
        logger.debug(msg);
    }

    @Override
    public void debug(LogMessage logMessage) {
        String msg = logMessage.getMessage();
        Throwable t = logMessage.getThrowable();
        Object[] os = logMessage.getObjects();

        if (os != null && t != null) {
            debug(msg, os, t);
        } else if (os != null) {
            debug(msg, os);
        } else if (t != null) {
            debug(msg, t);
        } else {
            debug(msg);
        }
    }

    @Override
    public void log(LogLevel level, String msg, Throwable t) {
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

    @Override
    public void log(LogLevel level, String msg, Object[] os) {
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

    @Override
    public void log(LogLevel level, String msg) {
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

    @Override
    public void log(LogLevel level, String msg, Object[] os, Throwable t) {
        switch (level) {
            case DEBUG:
                debug(msg, os, t);
                break;
            case ERROR:
            case FATAL:
                error(msg, os, t);
                break;
            case INFO:
                info(msg, os, t);
                break;
            case TRACE:
                trace(msg, os, t);
                break;
            case WARN:
                warn(msg, os, t);
                break;
        }
    }

    @Override
    public void log(LogMessage message) {
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
}
