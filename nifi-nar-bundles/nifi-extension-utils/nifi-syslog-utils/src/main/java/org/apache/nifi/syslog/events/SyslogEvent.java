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

/**
 * Encapsulates the parsed information for a single Syslog event.
 */
public class SyslogEvent {

    private final String priority;
    private final String severity;
    private final String facility;
    private final String version;
    private final String timeStamp;
    private final String hostName;
    private final String sender;
    private final String msgBody;
    private final String fullMessage;
    private final byte[] rawMessage;
    private final boolean valid;

    private SyslogEvent(final Builder builder) {
        this.priority = builder.priority;
        this.severity = builder.severity;
        this.facility = builder.facility;
        this.version = builder.version;
        this.timeStamp = builder.timeStamp;
        this.hostName = builder.hostName;
        this.sender = builder.sender;
        this.msgBody = builder.msgBody;
        this.fullMessage = builder.fullMessage;
        this.rawMessage = builder.rawMessage;
        this.valid = builder.valid;
    }

    public String getPriority() {
        return priority;
    }

    public String getSeverity() {
        return severity;
    }

    public String getFacility() {
        return facility;
    }

    public String getVersion() {
        return version;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public String getHostName() {
        return hostName;
    }

    public String getSender() {
        return sender;
    }

    public String getMsgBody() {
        return msgBody;
    }

    public String getFullMessage() {
        return fullMessage;
    }

    public byte[] getRawMessage() {
        return rawMessage;
    }

    public boolean isValid() {
        return valid;
    }

    public static final class Builder {
        private String priority;
        private String severity;
        private String facility;
        private String version;
        private String timeStamp;
        private String hostName;
        private String sender;
        private String msgBody;
        private String fullMessage;
        private byte[] rawMessage;
        private boolean valid;

        public void reset() {
            this.priority = null;
            this.severity = null;
            this.facility = null;
            this.version = null;
            this.timeStamp = null;
            this.hostName = null;
            this.sender = null;
            this.msgBody = null;
            this.fullMessage = null;
            this.valid = false;
        }

        public Builder priority(String priority) {
            this.priority = priority;
            return this;
        }

        public Builder severity(String severity) {
            this.severity = severity;
            return this;
        }

        public Builder facility(String facility) {
            this.facility = facility;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder timestamp(String timestamp) {
            this.timeStamp = timestamp;
            return this;
        }

        public Builder hostname(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public Builder sender(String sender) {
            this.sender = sender;
            return this;
        }

        public Builder msgBody(String msgBody) {
            this.msgBody = msgBody;
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

        public SyslogEvent build() {
            return new SyslogEvent(this);
        }
    }

}
