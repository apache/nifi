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
package org.apache.nifi.kafka.shared.property;

import org.apache.nifi.components.DescribedValue;

/**
 * Enumeration of supported Kafka Key Encoding Strategies
 */
public enum KeyEncoding implements DescribedValue {
    UTF8("utf-8", "UTF-8 Encoded", "The key is interpreted as a UTF-8 Encoded string."),
    HEX("hex", "Hex Encoded", "The key is interpreted as arbitrary binary data and is encoded using hexadecimal characters with uppercase letters"),
    DO_NOT_ADD("do-not-add", "Do Not Add Key as Attribute","The key will not be added as an Attribute");

    private final String value;
    private final String displayName;
    private final String description;

    KeyEncoding(final String value, final String displayName, final String description) {
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
}
