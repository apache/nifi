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

package org.apache.nifi.minifi.bootstrap;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.util.ArrayList;

/**
 * Helper class which makes possible to subscribe to logging events, and make assertions based on it.
 * With this approach we can avoid static mocking of loggers, which is impossible in some cases.
 */
public class TestLogAppender extends AppenderBase<ILoggingEvent> {

    private final ArrayList<ILoggingEvent> loggingEvents = new ArrayList<>();

    public void reset() {
        loggingEvents.clear();
    }

    public ILoggingEvent getLastLoggedEvent() {
        if (loggingEvents.isEmpty()) return null;

        return loggingEvents.get(loggingEvents.size() - 1);
    }

    public boolean containsMessage(String message) {
        return loggingEvents.stream()
            .anyMatch(event -> event.getMessage().equals(message));
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        loggingEvents.add(eventObject);
    }
}
