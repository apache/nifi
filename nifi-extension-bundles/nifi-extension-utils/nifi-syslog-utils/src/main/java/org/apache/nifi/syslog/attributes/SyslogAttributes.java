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
package org.apache.nifi.syslog.attributes;

import org.apache.nifi.flowfile.attributes.FlowFileAttributeKey;

/**
 * FlowFile Attributes for each Syslog message.
 */
public enum SyslogAttributes implements FlowFileAttributeKey {

    SYSLOG_PRIORITY("syslog.priority"),
    SYSLOG_SEVERITY("syslog.severity"),
    SYSLOG_FACILITY("syslog.facility"),
    SYSLOG_VERSION("syslog.version"),
    SYSLOG_TIMESTAMP("syslog.timestamp"),
    SYSLOG_HOSTNAME("syslog.hostname"),
    SYSLOG_SENDER("syslog.sender"),
    SYSLOG_BODY("syslog.body"),
    SYSLOG_VALID("syslog.valid"),
    SYSLOG_PROTOCOL("syslog.protocol"),
    SYSLOG_PORT("syslog.port"),

    PRIORITY("priority"),
    SEVERITY("severity"),
    FACILITY("facility"),
    VERSION("version"),
    TIMESTAMP("timestamp"),
    HOSTNAME("hostname"),
    SENDER("sender"),
    BODY("body"),
    VALID("valid"),
    PROTOCOL("protocol"),
    PORT("port");

    private String key;

    SyslogAttributes(String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
