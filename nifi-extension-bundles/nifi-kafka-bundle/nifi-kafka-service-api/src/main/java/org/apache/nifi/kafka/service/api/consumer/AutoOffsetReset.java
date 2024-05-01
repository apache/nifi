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
package org.apache.nifi.kafka.service.api.consumer;

import org.apache.nifi.components.DescribedValue;

/**
 * Kafka Consumer initial offset property when no offset exists
 */
public enum AutoOffsetReset implements DescribedValue {
    EARLIEST("earliest", "Automatically reset the offset to the earliest offset"),

    LATEST("latest", "Automatically reset the offset to the latest offset"),

    NONE("none", "Throw exception to the consumer if no previous offset found for the consumer group");

    private final String value;

    private final String description;

    AutoOffsetReset(final String value, final String description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return value;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
