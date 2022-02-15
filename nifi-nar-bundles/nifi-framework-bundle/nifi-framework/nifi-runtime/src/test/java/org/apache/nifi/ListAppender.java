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
package org.apache.nifi;

import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.AppenderBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListAppender extends AppenderBase<LoggingEvent> {
    private static final List<LoggingEvent> LOGGING_EVENTS = Collections.synchronizedList(new ArrayList<>());

    public static List<LoggingEvent> getLoggingEvents() {
        return LOGGING_EVENTS;
    }

    public static void clear() {
        LOGGING_EVENTS.clear();
    }

    @Override
    protected void append(final LoggingEvent loggingEvent) {
        LOGGING_EVENTS.add(loggingEvent);
    }
}
