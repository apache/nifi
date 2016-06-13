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
package org.apache.nifi.cluster.event;

import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.reporting.Severity;

/**
 * Events describe the occurrence of something noteworthy. They record the event's source, a timestamp, a description, and a category.
 *
 *
 * @Immutable
 */
public class Event implements NodeEvent {

    private final String source;
    private final long timestamp;
    private final Severity severity;
    private final String message;

    /**
     * Creates an event with the current time as the timestamp and a category of "INFO".
     *
     * @param source the source
     * @param message the description
     */
    public Event(final String source, final String message) {
        this(source, message, Severity.INFO);
    }

    /**
     * Creates an event with the current time as the timestamp.
     *
     * @param source the source
     * @param message the description
     * @param severity the event severity
     */
    public Event(final String source, final String message, final Severity severity) {
        this(source, message, severity, new Date().getTime());
    }

    /**
     * Creates an event with the a category of "INFO".
     *
     * @param source the source
     * @param message the description
     * @param timestamp the time of occurrence
     */
    public Event(final String source, final String message, final long timestamp) {
        this(source, message, Severity.INFO, timestamp);
    }

    /**
     * Creates an event.
     *
     * @param source the source
     * @param message the description
     * @param severity the event category
     * @param timestamp the time of occurrence
     */
    public Event(final String source, final String message, final Severity severity, final long timestamp) {
        if (StringUtils.isBlank(source)) {
            throw new IllegalArgumentException("Source may not be empty or null.");
        } else if (StringUtils.isBlank(message)) {
            throw new IllegalArgumentException("Event message may not be empty or null.");
        } else if (severity == null) {
            throw new IllegalArgumentException("Event category may not be null.");
        } else if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp may not be negative: " + timestamp);
        }

        this.source = source;
        this.message = message;
        this.severity = severity;
        this.timestamp = timestamp;
    }

    @Override
    public Severity getSeverity() {
        return severity;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }
}
