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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogMessage;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SimpleProcessLogger implements ComponentLog {

    private static final String CAUSED_BY = String.format("%n- Caused by: ");

    private static final Throwable NULL_THROWABLE = null;

    private final Logger logger;
    private final LogRepository logRepository;
    private final Object component;

    public SimpleProcessLogger(final String componentId, final Object component) {
        this(component, LogRepositoryFactory.getRepository(componentId));
    }

    public SimpleProcessLogger(final Object component, final LogRepository logRepository) {
        this.logger = LoggerFactory.getLogger(component.getClass());
        this.logRepository = logRepository;
        this.component = component;
    }

    @Override
    public void warn(final String msg, final Throwable t) {
        if (isWarnEnabled()) {
            final String message = getFormattedMessage(msg, t);
            final Object[] repositoryArguments = getRepositoryArguments(t);

            if (t == null) {
                logger.warn(message, component);
                logRepository.addLogMessage(LogLevel.WARN, message, repositoryArguments);
            } else {
                logger.warn(message, component, t);
                logRepository.addLogMessage(LogLevel.WARN, message, repositoryArguments, t);
            }
        }
    }

    @Override
    public void warn(final String msg, final Object[] os) {
        final Throwable lastThrowable = findLastThrowable(os);
        warn(msg, removeLastThrowable(os, lastThrowable), lastThrowable);
    }

    @Override
    public void warn(final String msg, final Object[] os, final Throwable t) {
        if (isWarnEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] arguments = addComponent(os);

            if (t == null) {
                logger.warn(componentMessage, arguments);
                logRepository.addLogMessage(LogLevel.WARN, componentMessage, arguments);
            } else {
                logger.warn(componentMessage, arguments, t);
                logRepository.addLogMessage(LogLevel.WARN, getCausesMessage(msg), getComponentAndCauses(t), t);
            }
        }
    }

    @Override
    public void warn(final String msg) {
        warn(msg, NULL_THROWABLE);
    }

    @Override
    public void warn(final LogMessage logMessage) {
        if (isWarnEnabled()) {
            log(LogLevel.WARN, logMessage);
            logRepository.addLogMessage(logMessage);
        }
    }

    @Override
    public void trace(final String msg, final Throwable t) {
        if (isTraceEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] repositoryArguments = getRepositoryArguments(t);

            if (t == null) {
                logger.trace(componentMessage, component);
                logRepository.addLogMessage(LogLevel.TRACE, componentMessage, repositoryArguments);
            } else {
                logger.trace(componentMessage, component, t);
                logRepository.addLogMessage(LogLevel.TRACE, getCausesMessage(msg), repositoryArguments, t);
            }
        }
    }

    @Override
    public void trace(final String msg, final Object[] os) {
        final Throwable lastThrowable = findLastThrowable(os);
        trace(msg, removeLastThrowable(os, lastThrowable), lastThrowable);
    }

    @Override
    public void trace(final String msg) {
        trace(msg, NULL_THROWABLE);
    }

    @Override
    public void trace(final String msg, final Object[] os, final Throwable t) {
        if (isTraceEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] arguments = addComponent(os);

            if (t == null) {
                logger.trace(componentMessage, arguments);
                logRepository.addLogMessage(LogLevel.TRACE, componentMessage, arguments);
            } else {
                logger.trace(componentMessage, arguments, t);
                logRepository.addLogMessage(LogLevel.TRACE, getCausesMessage(msg), getComponentAndCauses(t), t);
            }
        }
    }

    @Override
    public void trace(final LogMessage logMessage) {
        if (isTraceEnabled()) {
            log(LogLevel.TRACE, logMessage);
            logRepository.addLogMessage(logMessage);
        }
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
    public void info(final String msg, final Throwable t) {
        if (isInfoEnabled()) {
            final String message = t == null ? getComponentMessage(msg) : getCausesMessage(msg);
            final Object[] repositoryArguments = getRepositoryArguments(t);

            if (t == null) {
                logger.info(message, component);
                logRepository.addLogMessage(LogLevel.INFO, message, repositoryArguments);
            } else {
                logger.info(message, component, t);
                logRepository.addLogMessage(LogLevel.INFO, message, repositoryArguments, t);
            }
        }
    }

    @Override
    public void info(final String msg, final Object[] os) {
        final Throwable lastThrowable = findLastThrowable(os);
        info(msg, removeLastThrowable(os, lastThrowable), lastThrowable);
    }

    @Override
    public void info(final String msg) {
        info(msg, NULL_THROWABLE);
    }

    @Override
    public void info(final String msg, final Object[] os, final Throwable t) {
        if (isInfoEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] arguments = addComponent(os);

            if (t == null) {
                logger.info(componentMessage, arguments);
                logRepository.addLogMessage(LogLevel.INFO, componentMessage, arguments);
            } else {
                logger.info(componentMessage, arguments, t);
                logRepository.addLogMessage(LogLevel.INFO, getCausesMessage(msg), getComponentAndCauses(t), t);
            }
        }
    }

    @Override
    public void info(LogMessage logMessage) {
        if (isInfoEnabled()) {
            log(LogLevel.INFO, logMessage);
            logRepository.addLogMessage(logMessage);
        }
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public void error(final String msg) {
        error(msg, NULL_THROWABLE);
    }

    @Override
    public void error(final String msg, final Throwable t) {
        if (isErrorEnabled()) {
            final String componentMessage = getComponentMessage(msg);

            if (t == null) {
                logger.error(componentMessage, component);
                logRepository.addLogMessage(LogLevel.ERROR, msg, new Object[]{component});
            } else {
                logger.error(componentMessage, component, t);
                logRepository.addLogMessage(LogLevel.ERROR, getCausesMessage(msg), getComponentAndCauses(t), t);
            }
        }
    }

    @Override
    public void error(final String msg, final Object[] os) {
        final Throwable lastThrowable = findLastThrowable(os);
        error(msg, removeLastThrowable(os, lastThrowable), lastThrowable);
    }

    @Override
    public void error(final String msg, final Object[] os, final Throwable t) {
        if (isErrorEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] arguments = addComponent(os);

            if (t == null) {
                logger.error(componentMessage, arguments);
                logRepository.addLogMessage(LogLevel.ERROR, componentMessage, arguments);
            } else {
                logger.error(componentMessage, arguments, t);
                logRepository.addLogMessage(LogLevel.ERROR, getCausesMessage(msg), getComponentAndCauses(t), t);
            }
        }
    }

    @Override
    public void error(final LogMessage logMessage) {
        if (isErrorEnabled()) {
            log(LogLevel.ERROR, logMessage);
            logRepository.addLogMessage(logMessage);
        }
    }

    @Override
    public void debug(final String msg, final Throwable t) {
        if (isDebugEnabled()) {
            final String componentMessage = getComponentMessage(msg);

            if (t == null) {
                logger.debug(componentMessage, component);
                logRepository.addLogMessage(LogLevel.DEBUG, msg, new Object[]{component});
            } else {
                logger.debug(componentMessage, component, t);
                logRepository.addLogMessage(LogLevel.DEBUG, getCausesMessage(msg), getComponentAndCauses(t), t);
            }
        }
    }

    @Override
    public void debug(final String msg, final Object[] os) {
        final Throwable lastThrowable = findLastThrowable(os);
        debug(msg, removeLastThrowable(os, lastThrowable), lastThrowable);
    }

    @Override
    public void debug(final String msg, final Object[] os, final Throwable t) {
        if (isDebugEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] arguments = addComponent(os);

            if (t == null) {
                logger.debug(componentMessage, arguments);
                logRepository.addLogMessage(LogLevel.DEBUG, componentMessage, arguments);
            } else {
                logger.debug(componentMessage, arguments, t);
                logRepository.addLogMessage(LogLevel.DEBUG, getCausesMessage(msg), getComponentAndCauses(t), t);
            }
        }
    }

    @Override
    public void debug(final String msg) {
        debug(msg, NULL_THROWABLE);
    }

    @Override
    public void debug(final LogMessage logMessage) {
        if (isDebugEnabled()) {
            log(LogLevel.DEBUG, logMessage);
            logRepository.addLogMessage(logMessage);
        }
    }

    @Override
    public void log(final LogLevel level, final String msg, final Throwable t) {
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
    public void log(final LogLevel level, final String msg, final Object[] os) {
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
    public void log(final LogLevel level, final String msg) {
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
    public void log(final LogLevel level, final String msg, final Object[] os, final Throwable t) {
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
    public void log(final LogMessage message) {
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

    private String getFormattedMessage(final String message, final Throwable throwable) {
        return throwable == null ? getComponentMessage(message) : getCausesMessage(message);
    }

    /**
     * Get arguments for Log Repository including a summary of Throwable causes when Throwable is found
     *
     * @param throwable Throwable instance or null
     * @return Arguments containing the component or the component and summary of Throwable causes
     */
    private Object[] getRepositoryArguments(final Throwable throwable) {
        return throwable == null ? new Object[]{component} : getComponentAndCauses(throwable);
    }

    private String getCausesMessage(final String message) {
        return String.format("{} %s: {}", message);
    }

    private String getComponentMessage(final String message) {
        return String.format("{} %s", message);
    }

    private Object[] getComponentAndCauses(final Throwable throwable) {
        final List<String> causes = new ArrayList<>();
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            causes.add(cause.toString());
        }
        final String causesFormatted = String.join(CAUSED_BY, causes);
        return new Object[]{component, causesFormatted};
    }

    private Object[] addComponent(final Object[] originalArgs) {
        return prependToArgs(originalArgs, component);
    }

    private Object[] prependToArgs(final Object[] originalArgs, final Object... toAdd) {
        final Object[] newArgs = new Object[originalArgs.length + toAdd.length];
        System.arraycopy(toAdd, 0, newArgs, 0, toAdd.length);
        System.arraycopy(originalArgs, 0, newArgs, toAdd.length, originalArgs.length);
        return newArgs;
    }

    private Throwable findLastThrowable(final Object[] arguments) {
        final Object lastArgument = (arguments == null || arguments.length == 0) ? null : arguments[arguments.length - 1];
        Throwable lastThrowable = null;
        if (lastArgument instanceof Throwable) {
            lastThrowable = (Throwable) lastArgument;
        }
        return lastThrowable;
    }

    private Object[] removeLastThrowable(final Object[] arguments, final Throwable lastThrowable) {
        return lastThrowable == null ? arguments : ArrayUtils.removeElement(arguments, lastThrowable);
    }
}
