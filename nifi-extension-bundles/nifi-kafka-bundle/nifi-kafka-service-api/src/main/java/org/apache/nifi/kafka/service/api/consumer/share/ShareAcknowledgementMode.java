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
 * Acknowledgement mode for a Kafka share-group consumer. Maps to the underlying Kafka client
 * property {@code share.acknowledgement.mode}.
 */
public enum ShareAcknowledgementMode implements DescribedValue {
    IMPLICIT(
            "implicit",
            "Implicit",
            "Records delivered by a poll are implicitly accepted when the next poll is called or when commit is invoked."
                    + " Per-record failure handling is not available in this mode."
    ),

    EXPLICIT(
            "explicit",
            "Explicit",
            "Each delivered record must be acknowledged before the next poll. Enables per-record failure handling"
                    + " and selective release or rejection of records."
    );

    private final String value;
    private final String displayName;
    private final String description;

    ShareAcknowledgementMode(final String value, final String displayName, final String description) {
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
