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
package org.apache.nifi.processor;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProcessLogger implements ComponentLog {

    private final Logger logger;
    private final LogRepository logRepository;
    private final Object component;

    public SimpleProcessLogger(final String componentId, final Object component) {
        this.logger = LoggerFactory.getLogger(component.getClass());
        this.logRepository = LogRepositoryFactory.getRepository(componentId);
        this.component = component;
    }

    private Object[] addProcessor(final Object[] originalArgs) {
        return prependToArgs(originalArgs, component);
    }

    private Object[] prependToArgs(final Object[] originalArgs, final Object... toAdd) {
        final Object[] newArgs = new Object[originalArgs.length + toAdd.length];
        System.arraycopy(toAdd, 0, newArgs, 0, toAdd.length);
        System.arraycopy(originalArgs, 0, newArgs, toAdd.length, originalArgs.length);
        return newArgs;
    }

    private boolean lastArgIsException(final Object[] os) {
        return (os != null && os.length > 0 && (os[os.length - 1] instanceof Throwable));
    }

    @Override
    public void warn(String msg, final Throwable t) {
        if (!isWarnEnabled()) {
            return;
        }

        msg = "{} " + msg;
        final Object[] os = {component, t.toString(), t};
        logger.warn(msg, os);
        logRepository.addLogMessage(LogLevel.WARN, msg, os, t);
    }

    @Override
    public void warn(String msg, Object[] os) {
        if (!isWarnEnabled()) {
            return;
        }

        if (lastArgIsException(os)) {
            warn(msg, Arrays.copyOfRange(os, 0, os.length - 1), (Throwable) os[os.length - 1]);
        } else {
            msg = "{} " + msg;
            os = addProcessor(os);
            logger.warn(msg, os);
            logRepository.addLogMessage(LogLevel.WARN, msg, os);
        }
    }

    @Override
    public void warn(String msg, Object[] os, final Throwable t) {
        if (!isWarnEnabled()) {
            return;
        }

        os = addProcessorAndThrowable(os, t, logger.isDebugEnabled());
        msg = "{} " + msg + ": {}";
        logger.warn(msg, os);
        logRepository.addLogMessage(LogLevel.WARN, msg, os, t);
    }

    @Override
    public void warn(String msg) {
        if (!isWarnEnabled()) {
            return;
        }

        msg = "{} " + msg;
        final Object[] os = {component};
        logger.warn(msg, component);
        logRepository.addLogMessage(LogLevel.WARN, msg, os);
    }

    @Override
    public void trace(String msg, Throwable t) {
        if (!isTraceEnabled()) {
            return;
        }

        msg = "{} " + msg;
        final Object[] os = {component, t.toString(), t};
        logger.trace(msg, os);
        logRepository.addLogMessage(LogLevel.TRACE, msg, os, t);
    }

    @Override
    public void trace(String msg, Object[] os) {
        if (!isTraceEnabled()) {
            return;
        }

        msg = "{} " + msg;
        os = addProcessor(os);
        logger.trace(msg, os);
        logRepository.addLogMessage(LogLevel.TRACE, msg, os);
    }

    @Override
    public void trace(String msg) {
        if (!isTraceEnabled()) {
            return;
        }

        msg = "{} " + msg;
        final Object[] os = {component};
        logger.trace(msg, os);
        logRepository.addLogMessage(LogLevel.TRACE, msg, os);
    }

    @Override
    public void trace(String msg, Object[] os, Throwable t) {
        if (!isTraceEnabled()) {
            return;
        }

        os = addProcessorAndThrowable(os, t, true);
        msg = "{} " + msg + ": {}";

        logger.trace(msg, os);
        logRepository.addLogMessage(LogLevel.TRACE, msg, os, t);
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled() || logRepository.isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled() || logRepository.isInfoEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled() || logRepository.isWarnEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled() || logRepository.isErrorEnabled();
    }

    @Override
    public void info(String msg, Throwable t) {
        if (!isInfoEnabled()) {
            return;
        }

        msg = "{} " + msg;
        final Object[] os = {component, t.toString()};

        logger.info(msg, os);
        if (logger.isDebugEnabled()) {
            logger.info("", t);
        }
        logRepository.addLogMessage(LogLevel.INFO, msg, os, t);
    }

    @Override
    public void info(String msg, Object[] os) {
        if (!isInfoEnabled()) {
            return;
        }

        msg = "{} " + msg;
        os = addProcessor(os);

        logger.info(msg, os);
        logRepository.addLogMessage(LogLevel.INFO, msg, os);
    }

    @Override
    public void info(String msg) {
        if (!isInfoEnabled()) {
            return;
        }

        msg = "{} " + msg;
        final Object[] os = {component};

        logger.info(msg, os);
        logRepository.addLogMessage(LogLevel.INFO, msg, os);
    }

    @Override
    public void info(String msg, Object[] os, Throwable t) {
        if (!isInfoEnabled()) {
            return;
        }

        os = addProcessorAndThrowable(os, t, logger.isDebugEnabled());
        msg = "{} " + msg + ": {}";

        logger.info(msg, os);
        logRepository.addLogMessage(LogLevel.INFO, msg, os, t);
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public void error(String msg, Throwable t) {
        if (!isErrorEnabled()) {
            return;
        }

        if (t == null) {
            msg = "{} " + msg;
            final Object[] os = new Object[] {component};
            logger.error(msg, os);
            logRepository.addLogMessage(LogLevel.ERROR, msg, os);
        } else {
            msg = "{} " + msg + ": {}";
            final Object[] os = new Object[] {component, t.toString(), t};
            logger.error(msg, os);
            logRepository.addLogMessage(LogLevel.ERROR, msg, os, t);
        }
    }

    @Override
    public void error(String msg, Object[] os) {
        if (!isErrorEnabled()) {
            return;
        }

        if (lastArgIsException(os)) {
            error(msg, Arrays.copyOfRange(os, 0, os.length - 1), (Throwable) os[os.length - 1]);
        } else {
            os = addProcessor(os);
            msg = "{} " + msg;
            logger.error(msg, os);
            logRepository.addLogMessage(LogLevel.ERROR, msg, os);
        }
    }

    @Override
    public void error(String msg) {
        this.error(msg, (Throwable) null);
    }

    @Override
    public void error(String msg, Object[] os, Throwable t) {
        if (!isErrorEnabled()) {
            return;
        }

        os = addProcessorAndThrowable(os, t, true);
        msg = "{} " + msg + ": {}";

        logger.error(msg, os);
        logRepository.addLogMessage(LogLevel.ERROR, msg, os, t);
    }

    private Object[] addProcessorAndThrowable(final Object[] os, final Throwable t, final boolean includeStackTrace) {
        final Object[] modifiedArgs;
        if (t == null || !includeStackTrace) {
            modifiedArgs = new Object[os.length + 2];
            modifiedArgs[0] = component.toString();
            System.arraycopy(os, 0, modifiedArgs, 1, os.length);
            modifiedArgs[modifiedArgs.length - 1] = StringUtils.EMPTY;
        } else {
            modifiedArgs = new Object[os.length + 3];
            modifiedArgs[0] = component.toString();
            System.arraycopy(os, 0, modifiedArgs, 1, os.length);
            modifiedArgs[modifiedArgs.length - 2] = t.toString();
            modifiedArgs[modifiedArgs.length - 1] = t;
        }

        return modifiedArgs;
    }

    @Override
    public void debug(String msg, Throwable t) {
        if (!isDebugEnabled()) {
            return;
        }

        msg = "{} " + msg;
        final Object[] os = {component};

        logger.debug(msg, os, t);
        logRepository.addLogMessage(LogLevel.DEBUG, msg, os, t);
    }

    @Override
    public void debug(String msg, Object[] os) {
        if (!isDebugEnabled()) {
            return;
        }

        os = addProcessor(os);
        msg = "{} " + msg;

        logger.debug(msg, os);
        logRepository.addLogMessage(LogLevel.DEBUG, msg, os);
    }

    @Override
    public void debug(String msg, Object[] os, Throwable t) {
        if (!isDebugEnabled()) {
            return;
        }

        os = addProcessorAndThrowable(os, t, true);
        msg = "{} " + msg + ": {}";

        logger.debug(msg, os);
        logRepository.addLogMessage(LogLevel.DEBUG, msg, os, t);
    }

    @Override
    public void debug(String msg) {
        if (!isDebugEnabled()) {
            return;
        }

        msg = "{} " + msg;
        final Object[] os = {component};

        logger.debug(msg, os);
        logRepository.addLogMessage(LogLevel.DEBUG, msg, os);
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

}
