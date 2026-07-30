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
package org.apache.nifi.kafka.service.api.consumer.share;

import org.apache.nifi.components.DescribedValue;

/**
 * Acknowledgement type for a Kafka share-group record. Mirrors the broker-side outcomes
 * defined by KIP-932 without leaking the Kafka client API into the NiFi service contract.
 */
public enum Acknowledgement implements DescribedValue {
    ACCEPT("Accept", "Acknowledge successful processing of the record. The record will not be redelivered."),

    RELEASE("Release", "Release the record so that another consumer in the share group may receive it."),

    REJECT("Reject", "Reject the record as unprocessable. The record will not be redelivered to any consumer.");

    private final String displayName;
    private final String description;

    Acknowledgement(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
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
