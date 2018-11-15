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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardLogRepository implements LogRepository {

    public static final int DEFAULT_MAX_CAPACITY_PER_LEVEL = 10;

    private final Map<LogLevel, Collection<LogObserver>> observers = new HashMap<>();
    private final Map<String, LogObserver> observerLookup = new HashMap<>();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final Logger logger = LoggerFactory.getLogger(StandardLogRepository.class);

    private volatile ComponentLog componentLogger;

    @Override
    public void addLogMessage(final LogLevel level, final String message) {
        addLogMessage(level, message, (Throwable) null);
    }

    @Override
    public void addLogMessage(final LogLevel level, final String message, final Throwable t) {
        final LogMessage logMessage = new LogMessage(System.currentTimeMillis(), level, message, t);

        final Collection<LogObserver> logObservers = observers.get(level);
        if (logObservers != null) {
            for (LogObserver observer : logObservers) {
                try {
                    observer.onLogMessage(logMessage);
                } catch (final Throwable observerThrowable) {
                    logger.error("Failed to pass log message to Observer {} due to {}", observer, observerThrowable.toString());
                }
            }
        }
    }

    @Override
    public void addLogMessage(final LogLevel level, final String format, final Object[] params) {
        replaceThrowablesWithMessage(params);
        final String formattedMessage = MessageFormatter.arrayFormat(format, params).getMessage();
        addLogMessage(level, formattedMessage);
    }

    @Override
    public void addLogMessage(final LogLevel level, final String format, final Object[] params, final Throwable t) {
        replaceThrowablesWithMessage(params);
        final String formattedMessage = MessageFormatter.arrayFormat(format, params, t).getMessage();
        addLogMessage(level, formattedMessage, t);
    }

    private void replaceThrowablesWithMessage(Object[] params) {
        for (int i = 0; i < params.length; i++) {
            if(params[i] instanceof Throwable) {
                params[i] = ((Throwable) params[i]).getLocalizedMessage();
            }
        }
    }

    @Override
    public void setObservationLevel(String observerIdentifier, LogLevel level) {
        writeLock.lock();
        try {
            final LogObserver observer = removeObserver(observerIdentifier);

            if (observer != null) {
                addObserver(observerIdentifier, level, observer);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public LogLevel getObservationLevel(String observerIdentifier) {
        readLock.lock();
        try {
            // ensure observer exists
            if (!observerLookup.containsKey(observerIdentifier)) {
                throw new IllegalStateException("The specified observer identifier does not exist.");
            }

            final LogObserver observer = observerLookup.get(observerIdentifier);
            for (final LogLevel logLevel : LogLevel.values()) {
                final Collection<LogObserver> levelObservers = observers.get(logLevel);
                if (levelObservers != null && levelObservers.contains(observer)) {
                    return logLevel;
                }
            }

            // at this point, the LogLevel must be NONE since we don't register observers for NONE
            return LogLevel.NONE;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addObserver(final String observerIdentifier, final LogLevel minimumLevel, final LogObserver observer) {
        writeLock.lock();
        try {
            // ensure observer does not exists
            if (observerLookup.containsKey(observerIdentifier)) {
                throw new IllegalStateException("The specified observer identifier already exists.");
            }

            final LogLevel[] allLevels = LogLevel.values();
            for (int i = minimumLevel.ordinal(); i < allLevels.length; i++) {
                // no need to register an observer for NONE since that level will never be logged to by a component
                if (i != LogLevel.NONE.ordinal()) {
                    Collection<LogObserver> collection = observers.get(allLevels[i]);
                    if (collection == null) {
                        collection = new ArrayList<>();
                        observers.put(allLevels[i], collection);
                    }
                    collection.add(observer);
                }
            }
            observerLookup.put(observerIdentifier, observer);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public LogObserver removeObserver(final String observerIdentifier) {
        writeLock.lock();
        try {
            final LogObserver observer = observerLookup.get(observerIdentifier);
            for (final Collection<LogObserver> collection : observers.values()) {
                collection.remove(observer);
            }
            return observerLookup.remove(observerIdentifier);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeAllObservers() {
        writeLock.lock();
        try {
            observers.clear();
            observerLookup.clear();
        } finally {
            writeLock.unlock();
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
        final Collection<LogObserver> logLevelObservers = observers.get(logLevel);
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
