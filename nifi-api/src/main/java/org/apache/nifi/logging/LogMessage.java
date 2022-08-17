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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class LogMessage {

    private final long time;
    private final String message;
    private final LogLevel logLevel;
    private final Throwable throwable;
    private final String flowFileUuid;
    private final Object[] objects;

    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String TO_STRING_FORMAT = "%1$s %2$s - %3$s";

    public static class Builder {

        private final long time;
        private final LogLevel logLevel;
        private String message;
        private Throwable throwable;
        private String flowFileUuid;
        private Object[] objects;

        public Builder(final long time, final LogLevel logLevel) {
            this.time = time;
            this.logLevel = logLevel;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder throwable(Throwable throwable) {
            this.throwable = throwable;
            return this;
        }

        public Builder flowFileUuid(String flowFileUuid) {
            this.flowFileUuid = flowFileUuid;
            return this;
        }

        public Builder objects(Object[] objects) {
            this.objects = objects;
            return this;
        }

        public LogMessage createLogMessage() {
            return new LogMessage(time, logLevel, message, throwable, flowFileUuid, objects);
        }
    }

    private LogMessage(final long time, final LogLevel logLevel, final String message, final Throwable throwable, final String flowFileUuid, final Object[] objects) {
        this.logLevel = logLevel;
        this.throwable = throwable;
        this.message = message;
        this.time = time;
        this.flowFileUuid = flowFileUuid;
        this.objects = objects;
    }

    public long getTime() {
        return time;
    }

    public String getMessage() {
        return message;
    }

    public LogLevel getLogLevel() {
        return logLevel;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public String getFlowFileUuid() {
        return flowFileUuid;
    }

    public Object[] getObjects() {
        return objects;
    }

    @Override
    public String toString() {
        final DateFormat dateFormat = new SimpleDateFormat(DATE_TIME_FORMAT, Locale.US);
        final String formattedTime = dateFormat.format(new Date(time));

        String formattedMsg = String.format(TO_STRING_FORMAT, formattedTime, logLevel.toString(), message);
        if (throwable != null) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            throwable.printStackTrace(pw);
            formattedMsg += System.lineSeparator() + sw;
        }

        return formattedMsg;
    }
}
