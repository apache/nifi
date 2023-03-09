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

public class NopLogRepository implements LogRepository {
    private volatile ComponentLog logger;

    @Override
    public void addLogMessage(LogMessage logMessage) {
    }

    @Override
    public void addLogMessage(final LogLevel level, final String messageFormat, final Object[] params) {
    }

    @Override
    public void addLogMessage(final LogLevel level, final String messageFormat, final Object[] params, final Throwable t) {
    }

    @Override
    public void addObserver(final LogLevel level, final LogObserver observer) {
    }

    @Override
    public void setObservationLevel(final LogLevel level) {
    }


    @Override
    public void removeAllObservers() {
    }

    @Override
    public void setLogger(final ComponentLog logger) {
        this.logger = logger;
    }

    @Override
    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public boolean isInfoEnabled() {
        return true;
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public boolean isErrorEnabled() {
        return true;
    }
}
