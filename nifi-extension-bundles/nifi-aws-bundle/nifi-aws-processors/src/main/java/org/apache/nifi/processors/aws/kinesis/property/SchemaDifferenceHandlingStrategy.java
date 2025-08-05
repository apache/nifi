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
package org.apache.nifi.processors.aws.kinesis.property;

import org.apache.nifi.components.DescribedValue;

public enum SchemaDifferenceHandlingStrategy implements DescribedValue {
    CREATE_FLOW_FILE("Create FlowFile", "Create a new FlowFile for each record with a different schema. The previous FlowFile will be completed with the records that have the previous schema." +
            " Emitted FlowFiles will contain continuous record sequences."),
    GROUP_RECORDS("Group Records By Schema", "Group records with the same schema into a single FlowFile. If a record with a different schema is encountered, a new FlowFile will be created for" +
            " the new schema. Emitted FlowFiles may contain non-sequential records. This strategy is useful when the schema changes frequently and highest performance is required.");

    private final String displayName;
    private final String description;

    SchemaDifferenceHandlingStrategy(final String displayName, final String description) {
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
