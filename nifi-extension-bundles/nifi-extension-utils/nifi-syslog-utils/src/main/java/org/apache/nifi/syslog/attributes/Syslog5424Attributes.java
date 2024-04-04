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
public enum Syslog5424Attributes implements FlowFileAttributeKey {

    SYSLOG_APP_NAME("syslog.appName"),
    SYSLOG_PROCID("syslog.procid"),
    SYSLOG_MESSAGEID("syslog.messageid"),
    SYSLOG_STRUCTURED_BASE("syslog.structuredData"),
    SYSLOG_STRUCTURED_ELEMENT_ID_FMT("syslog.structuredData.%s"),
    SYSLOG_STRUCTURED_ELEMENT_ID_PNAME_FMT("syslog.structuredData.%s.%s"),
    SYSLOG_STRUCTURED_ELEMENT_ID_PNAME_PATTERN("syslog.structuredData\\.(.*)\\.(.*)$"),
    APP_NAME("appName"),
    PROCID("procid"),
    MESSAGEID("messageid"),
    STRUCTURED_BASE("structuredData"),
    STRUCTURED_ELEMENT_ID_FMT("structuredData.%s"),
    STRUCTURED_ELEMENT_ID_PNAME_FMT("structuredData.%s.%s"),
    STRUCTURED_ELEMENT_ID_PNAME_PATTERN("structuredData\\.(.*)\\.(.*)$");
    private String key;

    Syslog5424Attributes(String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
