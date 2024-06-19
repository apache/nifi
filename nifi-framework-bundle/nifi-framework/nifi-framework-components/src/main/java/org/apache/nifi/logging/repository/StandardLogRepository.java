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
package org.apache.nifi.logging.repository;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogMessage;
import org.apache.nifi.logging.LogObserver;
import org.apache.nifi.logging.LogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StandardLogRepository implements LogRepository {

    private final Map<LogLevel, Collection<LogObserver>> observersPerLogLevel = new ConcurrentHashMap<>();
    private final Set<LogObserver> observers = new HashSet<>();

    private final Lock lock = new ReentrantLock();

    private final Logger logger = LoggerFactory.getLogger(StandardLogRepository.class);

    private volatile ComponentLog componentLogger;

    @Override
    public void addLogMessage(LogMessage logMessage) {
        LogLevel logLevel = logMessage.getLogLevel();

        final Collection<LogObserver> logObservers = observersPerLogLevel.get(logLevel);
        if (logObservers != null) {
            for (LogObserver observer : logObservers) {
                try {
                    observer.onLogMessage(logMessage);
                } catch (final Exception observerThrowable) {
                    logger.error("Failed to pass log message to Observer {}", observer, observerThrowable);
                }
            }
        }

    }

    @Override
    public void addLogMessage(final LogLevel level, final String format, final Object[] params) {
        final Optional<String> flowFileUuid = getFirstFlowFileUuidFromObjects(params);
        simplifyArgs(params);
        final String formattedMessage = MessageFormatter.arrayFormat(format, params).getMessage();
        final LogMessage logMessage = new LogMessage.Builder(System.currentTimeMillis(), level)
                .message(formattedMessage)
                .flowFileUuid(flowFileUuid.orElse(null))
                .createLogMessage();
        addLogMessage(logMessage);
    }

    @Override
    public void addLogMessage(final LogLevel level, final String format, final Object[] params, final Throwable t) {
        final Optional<String> flowFileUuid = getFirstFlowFileUuidFromObjects(params);
        simplifyArgs(params);
        final String formattedMessage = MessageFormatter.arrayFormat(format, params, t).getMessage();
        final LogMessage logMessage = new LogMessage.Builder(System.currentTimeMillis(), level)
                .message(formattedMessage)
                .throwable(t)
                .flowFileUuid(flowFileUuid.orElse(null))
                .createLogMessage();
        addLogMessage(logMessage);
    }

    private Optional<String> getFirstFlowFileUuidFromObjects(Object[] params) {
        int flowFileCount = 0;
        FlowFile flowFileFound = null;
        for (final Object param : params) {
            if (param instanceof FlowFile) {
                if (++flowFileCount > 1) {
                    return Optional.empty();
                }
                flowFileFound = (FlowFile) param;
            }
        }
        return Optional.ofNullable(flowFileFound).map(ff -> ff.getAttribute(CoreAttributes.UUID.key()));
    }

    private void simplifyArgs(final Object[] params) {
        for (int i = 0; i < params.length; i++) {
            params[i] = simplifyArg(params[i]);
        }
    }

    private Object simplifyArg(final Object param) {
        if (param instanceof Throwable) {
            return ((Throwable) param).getLocalizedMessage();
        } else if (param instanceof FlowFile) {
            final FlowFile flowFile = (FlowFile) param;
            return "FlowFile[filename=" + flowFile.getAttribute(CoreAttributes.FILENAME.key()) + "]";
        }

        return param;
    }

    @Override
    public void setObservationLevel(LogLevel level) {
        lock.lock();
        try {
            final Set<LogObserver> observersCopy = new HashSet<>(observers);
            observers.clear();
            observersPerLogLevel.clear();

            for (final LogObserver observer : observersCopy) {
                addObserver(level, observer);
            }
        } finally {
            lock.unlock();
        }
    }


    @Override
    public void addObserver(final LogLevel minimumLevel, final LogObserver observer) {
        lock.lock();
        try {
            final LogLevel[] allLevels = LogLevel.values();
            for (int i = minimumLevel.ordinal(); i < allLevels.length; i++) {
                // no need to register an observer for NONE since that level will never be logged to by a component
                if (i != LogLevel.NONE.ordinal()) {
                    Collection<LogObserver> collection = observersPerLogLevel.computeIfAbsent(allLevels[i], k -> new ArrayList<>());
                    collection.add(observer);
                }
            }
            observers.add(observer);
        } finally {
            lock.unlock();
        }
    }


    @Override
    public void removeAllObservers() {
        lock.lock();
        try {
            observersPerLogLevel.clear();
            observers.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void setLogger(final ComponentLog componentLogger) {
        this.componentLogger = componentLogger;
    }

    @Override
    public ComponentLog getLogger() {
        return componentLogger;
    }

    private boolean hasObserver(final LogLevel logLevel) {
        final Collection<LogObserver> logLevelObservers = observersPerLogLevel.get(logLevel);
        return (logLevelObservers != null && !logLevelObservers.isEmpty());
    }

    @Override
    public boolean isDebugEnabled() {
        return hasObserver(LogLevel.DEBUG);
    }

    @Override
    public boolean isInfoEnabled() {
        return hasObserver(LogLevel.INFO);
    }

    @Override
    public boolean isWarnEnabled() {
        return hasObserver(LogLevel.WARN);
    }

    @Override
    public boolean isErrorEnabled() {
        return hasObserver(LogLevel.ERROR);
    }
}
