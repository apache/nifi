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
package org.apache.nifi.syslog.events;

import java.util.Map;

/**
 * Encapsulates the parsed information for a single Syslog 5424 event.
 */
public class Syslog5424Event {
    private final Map<String, Object> fieldMap;
    private final String fullMessage;
    private final byte[] rawMessage;
    private final String sender;
    private final boolean valid;
    private final Exception exception;

    private Syslog5424Event(final Builder builder) {
        this.fieldMap = builder.fieldMap;
        this.fullMessage = builder.fullMessage;
        this.rawMessage = builder.rawMessage;
        this.sender = builder.sender;
        this.valid = builder.valid;
        this.exception = builder.exception;
    }

    public Map<String, Object> getFieldMap() {
        return fieldMap;
    }

    public Exception getException() {
        return exception;
    }

    public String getFullMessage() {
        return fullMessage;
    }

    public byte[] getRawMessage() {
        return rawMessage;
    }

    public String getSender() {
        return sender;
    }

    public boolean isValid() {
        return valid;
    }

    public static final class Builder {
        private Exception exception;
        private String fullMessage;
        private String sender;
        private Map<String, Object> fieldMap;
        private byte[] rawMessage;
        private boolean valid;

        public void reset() {
            this.fieldMap = null;
            this.sender = null;
            this.fullMessage = null;
            this.valid = false;
            this.exception = null;
        }

        public Builder sender(String sender) {
            this.sender = sender;
            return this;
        }

        public Builder exception(Exception exception) {
            this.exception = exception;
            return this;
        }

        public Builder fieldMap(Map<String, Object> fieldMap) {
            this.fieldMap = fieldMap;
            return this;
        }

        public Builder fullMessage(String fullMessage) {
            this.fullMessage = fullMessage;
            return this;
        }

        public Builder rawMessage(byte[] rawMessage) {
            this.rawMessage = rawMessage;
            return this;
        }

        public Builder valid(boolean valid) {
            this.valid = valid;
            return this;
        }

        public Syslog5424Event build() {
            return new Syslog5424Event(this);
        }
    }

}
