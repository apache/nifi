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

package org.apache.nifi.properties

import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.AppenderBase

import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

public class RecordingAppender extends AppenderBase<LoggingEvent> {
    private static final Lock LOCK = new ReentrantLock(false)
    private static List<LoggingEvent> events = null

    @Override
    protected void append(LoggingEvent e) {
        synchronized (RecordingAppender.class) {
            events.add(e)
        }
    }

    public static void reset() {
        synchronized (RecordingAppender.class) {
            events.clear()
        }
    }

    public static void acquire(List<LoggingEvent> events) {
        LOCK.lock()
        RecordingAppender.events = events;
    }

    public static void release() {
        RecordingAppender.events = null
        LOCK.unlock()
    }
}
