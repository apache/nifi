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
 * Enumeration of supported Kafka Output Strategies
 */
public enum OutputStrategy implements DescribedValue {
    USE_VALUE("USE_VALUE", "Use Content as Value", "Write only the Kafka Record value to the FlowFile record."),
    USE_WRAPPER("USE_WRAPPER", "Use Wrapper", "Write the Kafka Record key, value, headers, and metadata into the FlowFile record. (See processor's additional details for more information.)"),
    INJECT_METADATA("INJECT_METADATA", "Inject Metadata",
            "Write the Kafka Record value to the FlowFile record and add a sub-record to it with key, headers, and metadata. (See processor's additional details for more information.)");

    private final String value;
    private final String displayName;
    private final String description;

    OutputStrategy(final String value, final String displayName, final String description) {
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
