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
package org.apache.nifi.py4j.logging;

import org.apache.nifi.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Standard implementation of Log Level Change Handler with singleton instance for shared collection of listeners
 */
public class StandardLogLevelChangeHandler implements LogLevelChangeHandler {
    private static final StandardLogLevelChangeHandler HANDLER = new StandardLogLevelChangeHandler();

    private static final Logger handlerLogger = LoggerFactory.getLogger(StandardLogLevelChangeHandler.class);

    private final Map<String, LogLevelChangeListener> listeners = new ConcurrentHashMap<>();

    private final Map<String, LogLevel> loggerLevels = new ConcurrentHashMap<>();

    private StandardLogLevelChangeHandler() {

    }

    /**
     * Get shared reference to Handler for Listener registration and centralized notification
     *
     * @return Shared Log Level Change Handler
     */
    public static LogLevelChangeHandler getHandler() {
        return HANDLER;
    }

    /**
     * Register Log Level Change Listener with provided identifier
     *
     * @param identifier Tracking identifier associated with Log Level Change Listener
     * @param listener Log Level Change Listener to be registered
     */
    @Override
    public void addListener(final String identifier, final LogLevelChangeListener listener) {
        Objects.requireNonNull(identifier, "Identifier required");
        Objects.requireNonNull(listener, "Listener required");

        for (final Map.Entry<String, LogLevel> loggerLevel : loggerLevels.entrySet()) {
            listener.onLevelChange(loggerLevel.getKey(), loggerLevel.getValue());
        }

        listeners.put(identifier, listener);
        handlerLogger.trace("Added Listener [{}]", identifier);
    }

    /**
     * Remove registered listener based on provided identifier
     *
     * @param identifier Tracking identifier of registered listener to be removed
     */
    @Override
    public void removeListener(String identifier) {
        Objects.requireNonNull(identifier, "Identifier required");
        listeners.remove(identifier);
        handlerLogger.trace("Removed Listener [{}]", identifier);
    }

    /**
     * Handle level change notification for named logger and broadcast to registered Listeners
     *
     * @param loggerName Name of logger with updated level
     * @param logLevel New log level
     */
    @Override
    public void onLevelChange(final String loggerName, final LogLevel logLevel) {
        Objects.requireNonNull(loggerName, "Logger Name required");
        Objects.requireNonNull(logLevel, "Log Level required");
        handlerLogger.trace("Logger [{}] Level [{}] changed", loggerName, logLevel);

        loggerLevels.put(loggerName, logLevel);
        for (final LogLevelChangeListener listener : listeners.values()) {
            listener.onLevelChange(loggerName, logLevel);
        }
    }
}
