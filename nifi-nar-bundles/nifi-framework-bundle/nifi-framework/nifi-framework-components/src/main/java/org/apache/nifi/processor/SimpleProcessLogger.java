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
import org.apache.nifi.logging.LoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.List;

public class SimpleProcessLogger implements ComponentLog {

    private static final String CAUSED_BY = String.format("%n- Caused by: ");

    private static final Throwable NULL_THROWABLE = null;

    private final Logger logger;
    private final LogRepository logRepository;
    private final Object component;

    private final LoggingContext loggingContext;

    public SimpleProcessLogger(final String componentId, final Object component, final LoggingContext loggingContext) {
        this(component, LogRepositoryFactory.getRepository(componentId), loggingContext);
    }

    public SimpleProcessLogger(final Object component, final LogRepository logRepository, final LoggingContext loggingContext) {
        this.logger = LoggerFactory.getLogger(component.getClass());
        this.logRepository = logRepository;
        this.component = component;
        this.loggingContext = loggingContext;
    }

    @Override
    public void warn(final String msg, final Throwable t) {
        if (isWarnEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] repositoryArguments = getRepositoryArguments(t);

            if (t == null) {
                log(Level.WARN, componentMessage, component);
                logRepository.addLogMessage(LogLevel.WARN, componentMessage, repositoryArguments);
            } else {
                log(Level.WARN, componentMessage, component, t);
                logRepository.addLogMessage(LogLevel.WARN, getCausesMessage(msg), repositoryArguments, t);
            }
        }
    }

    @Override
    public void warn(final String msg, final Object... os) {
        if (isWarnEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] arguments = insertComponent(os);

            final Throwable lastThrowable = findLastThrowable(os);
            if (lastThrowable == null) {
                log(Level.WARN, componentMessage, arguments);
                logRepository.addLogMessage(LogLevel.WARN, componentMessage, arguments);
            } else {
                log(Level.WARN, componentMessage, setFormattedThrowable(arguments, lastThrowable));
                logRepository.addLogMessage(LogLevel.WARN, getCausesMessage(msg), setCauses(arguments, lastThrowable), lastThrowable);
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
                log(Level.TRACE, componentMessage, component);
                logRepository.addLogMessage(LogLevel.TRACE, componentMessage, repositoryArguments);
            } else {
                log(Level.TRACE, componentMessage, component, t);
                logRepository.addLogMessage(LogLevel.TRACE, getCausesMessage(msg), repositoryArguments, t);
            }
        }
    }

    @Override
    public void trace(final String msg, final Object... os) {
        if (isTraceEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] arguments = insertComponent(os);

            final Throwable lastThrowable = findLastThrowable(os);
            if (lastThrowable == null) {
                log(Level.TRACE, componentMessage, arguments);
                logRepository.addLogMessage(LogLevel.TRACE, componentMessage, arguments);
            } else {
                log(Level.TRACE, componentMessage, setFormattedThrowable(arguments, lastThrowable));
                logRepository.addLogMessage(LogLevel.TRACE, getCausesMessage(msg), setCauses(arguments, lastThrowable), lastThrowable);
            }
        }
    }

    @Override
    public void trace(final String msg) {
        trace(msg, NULL_THROWABLE);
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
            final String componentMessage = getComponentMessage(msg);
            final Object[] repositoryArguments = getRepositoryArguments(t);

            if (t == null) {
                log(Level.INFO, componentMessage, component);
                logRepository.addLogMessage(LogLevel.INFO, componentMessage, repositoryArguments);
            } else {
                log(Level.INFO, componentMessage, component, t);
                logRepository.addLogMessage(LogLevel.INFO, getCausesMessage(msg), repositoryArguments, t);
            }
        }
    }

    @Override
    public void info(final String msg, final Object... os) {
        if (isInfoEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] arguments = insertComponent(os);

            final Throwable lastThrowable = findLastThrowable(os);
            if (lastThrowable == null) {
                log(Level.INFO, componentMessage, arguments);
                logRepository.addLogMessage(LogLevel.INFO, componentMessage, arguments);
            } else {
                log(Level.INFO, componentMessage, setFormattedThrowable(arguments, lastThrowable));
                logRepository.addLogMessage(LogLevel.INFO, getCausesMessage(msg), setCauses(arguments, lastThrowable), lastThrowable);
            }
        }
    }

    @Override
    public void info(final String msg) {
        info(msg, NULL_THROWABLE);
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
            final Object[] repositoryArguments = getRepositoryArguments(t);

            if (t == null) {
                log(Level.ERROR, componentMessage, component);
                logRepository.addLogMessage(LogLevel.ERROR, componentMessage, repositoryArguments);
            } else {
                log(Level.ERROR, componentMessage, component, t);
                logRepository.addLogMessage(LogLevel.ERROR, getCausesMessage(msg), repositoryArguments, t);
            }
        }
    }

    @Override
    public void error(final String msg, final Object... os) {
        if (isErrorEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] arguments = insertComponent(os);

            final Throwable lastThrowable = findLastThrowable(os);
            if (lastThrowable == null) {
                log(Level.ERROR, componentMessage, arguments);
                logRepository.addLogMessage(LogLevel.ERROR, componentMessage, arguments);
            } else {
                log(Level.ERROR, componentMessage, setFormattedThrowable(arguments, lastThrowable));
                logRepository.addLogMessage(LogLevel.ERROR, getCausesMessage(msg), setCauses(arguments, lastThrowable), lastThrowable);
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
            final Object[] repositoryArguments = getRepositoryArguments(t);

            if (t == null) {
                log(Level.DEBUG, componentMessage, component);
                logRepository.addLogMessage(LogLevel.DEBUG, componentMessage, repositoryArguments);
            } else {
                log(Level.DEBUG, componentMessage, component, t);
                logRepository.addLogMessage(LogLevel.DEBUG, getCausesMessage(msg), repositoryArguments, t);
            }
        }
    }

    @Override
    public void debug(final String msg, final Object... os) {
        if (isDebugEnabled()) {
            final String componentMessage = getComponentMessage(msg);
            final Object[] arguments = insertComponent(os);

            final Throwable lastThrowable = findLastThrowable(os);
            if (lastThrowable == null) {
                log(Level.DEBUG, componentMessage, arguments);
                logRepository.addLogMessage(LogLevel.DEBUG, componentMessage, arguments);
            } else {
                log(Level.DEBUG, componentMessage, setFormattedThrowable(arguments, lastThrowable));
                logRepository.addLogMessage(LogLevel.DEBUG, getCausesMessage(msg), setCauses(arguments, lastThrowable), lastThrowable);
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
    public void log(final LogLevel level, final String msg, final Object... os) {
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
        final String causes = getCauses(throwable);
        return new Object[]{component, causes};
    }

    private String getCauses(final Throwable throwable) {
        final List<String> causes = new ArrayList<>();
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            causes.add(cause.toString());
        }
        return String.join(CAUSED_BY, causes);
    }

    private Object[] insertComponent(final Object[] originalArgs) {
        return ArrayUtils.insert(0, originalArgs, component);
    }

    private Object[] setCauses(final Object[] arguments, final Throwable throwable) {
        final String causes = getCauses(throwable);
        final int lastIndex = arguments.length - 1;
        final Object[] argumentsThrowableRemoved = ArrayUtils.remove(arguments, lastIndex);
        return ArrayUtils.add(argumentsThrowableRemoved, causes);
    }

    private Object[] setFormattedThrowable(final Object[] arguments, final Throwable throwable) {
        final int lastIndex = arguments.length - 1;
        final Object[] argumentsThrowableRemoved = ArrayUtils.remove(arguments, lastIndex);
        return ArrayUtils.addAll(argumentsThrowableRemoved, throwable.toString(), throwable);
    }

    private Throwable findLastThrowable(final Object[] arguments) {
        final Object lastArgument = (arguments == null || arguments.length == 0) ? null : arguments[arguments.length - 1];
        Throwable lastThrowable = null;
        if (lastArgument instanceof Throwable) {
            lastThrowable = (Throwable) lastArgument;
        }
        return lastThrowable;
    }

    private String getDiscriminatorKey() {
        return loggingContext.getDiscriminatorKey();
    }

    private String getLogFileSuffix() {
        return loggingContext.getLogFileSuffix().orElse(null);
    }

    private void log(final Level level, final String message, final Object... arguments) {
        logger.makeLoggingEventBuilder(level)
                .addKeyValue(getDiscriminatorKey(), getLogFileSuffix())
                .log(message, arguments);
    }
}
