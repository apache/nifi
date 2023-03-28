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
package org.apache.nifi.kafka.processors.producer.wrapper;

import org.apache.nifi.components.DescribedValue;

/**
 * On use of {@link org.apache.nifi.kafka.processors.PublishKafka} strategy USE_WRAPPER, provide configurability to
 * specify source of data for wrapper record metadata.
 */
public enum RecordMetadataStrategy implements DescribedValue {
    FROM_RECORD("Metadata From Record", "The Kafka Record's Topic and Partition will be "
            + "determined by looking at the /metadata/topic and /metadata/partition fields of the Record, "
            + "respectively. If these fields are invalid or not present, the Topic Name and Partition/Partitioner "
            + "class properties of the processor will be considered."),

    FROM_PROPERTIES("Use Configured Values", "The Kafka Record's Topic will be determined "
            + "using the 'Topic Name' processor property. The partition will be determined using the 'Partition' and "
            + "'Partitioner class' properties.");

    private final String displayName;

    private final String description;

    RecordMetadataStrategy(final String displayName, final String description) {
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
