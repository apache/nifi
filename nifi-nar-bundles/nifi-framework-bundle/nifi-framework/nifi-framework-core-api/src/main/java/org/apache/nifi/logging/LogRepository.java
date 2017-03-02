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

public interface LogRepository {

    void addLogMessage(LogLevel level, String message);

    void addLogMessage(LogLevel level, String message, Throwable t);

    void addLogMessage(LogLevel level, String messageFormat, Object[] params);

    void addLogMessage(LogLevel level, String messageFormat, Object[] params, Throwable t);

    /**
     * Registers an observer so that it will be notified of all Log Messages
     * whose levels are at least equal to the given level.
     *
     * @param observerIdentifier identifier of observer
     * @param level of logs the observer wants
     * @param observer the observer
     */
    void addObserver(String observerIdentifier, LogLevel level, LogObserver observer);

    /**
     * Sets the observation level of the specified observer.
     *
     * @param observerIdentifier identifier of observer
     * @param level of logs the observer wants
     */
    void setObservationLevel(String observerIdentifier, LogLevel level);

    /**
     * Gets the observation level for the specified observer.
     *
     * @param observerIdentifier identifier of observer
     * @return level
     */
    LogLevel getObservationLevel(String observerIdentifier);

    /**
     * Removes the given LogObserver from this Repository.
     *
     * @param observerIdentifier identifier of observer
     * @return old log observer
     */
    LogObserver removeObserver(String observerIdentifier);

    /**
     * Removes all LogObservers from this Repository
     */
    void removeAllObservers();

    /**
     * Sets the current logger for the component
     *
     * @param logger the logger to use
     */
    void setLogger(ComponentLog logger);

    /**
     * @return the current logger for the component
     */
    ComponentLog getLogger();

    boolean isDebugEnabled();

    boolean isInfoEnabled();

    boolean isWarnEnabled();

    boolean isErrorEnabled();
}
