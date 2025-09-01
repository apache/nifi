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
package org.apache.nifi.snmp.processors.properties;

import org.apache.nifi.components.DescribedValue;

public enum UsmUserInputMethod implements DescribedValue {

    USM_JSON_FILE_PATH("usm-json-file-path", "Json File Path", "The path of the JSON file containing the USM users"),
    USM_JSON_CONTENT("usm-json-content", "Json Content", "The JSON containing the USM users"),
    USM_SECURITY_NAMES("usm-security-names", "Security Names", "In case of noAuthNoPriv security level - the list of security names separated by commas");

    private final String value;
    private final String displayName;
    private final String description;

    UsmUserInputMethod(String value, String displayName, String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static UsmUserInputMethod fromValue(String value) {
        for (UsmUserInputMethod source : values()) {
            if (source.value.equals(value)) {
                return source;
            }
        }
        throw new IllegalArgumentException("Unknown value: " + value);
    }
}

