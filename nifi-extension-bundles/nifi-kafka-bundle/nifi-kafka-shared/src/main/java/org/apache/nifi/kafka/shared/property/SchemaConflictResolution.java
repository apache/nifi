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
 * Enumeration of supported strategies for resolving schema conflicts when batching
 * Kafka Records into FlowFiles using RECORD processing strategy.
 */
public enum SchemaConflictResolution implements DescribedValue {
    CREATE_NEW_FLOWFILE("Create New FlowFile",
            "When records have different schemas, a new FlowFile is created for each distinct schema."),

    CONTINUE_WITH_MERGED_SCHEMA("Continue with Merged Schema",
            "When records have different schemas, all schemas within the same topic/partition group are merged "
                    + "into a single write schema so that all records are written into a single FlowFile.");

    private final String displayName;
    private final String description;

    SchemaConflictResolution(final String displayName, final String description) {
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
