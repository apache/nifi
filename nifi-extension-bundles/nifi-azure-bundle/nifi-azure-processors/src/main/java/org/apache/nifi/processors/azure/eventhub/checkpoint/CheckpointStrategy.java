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
package org.apache.nifi.processors.azure.eventhub.checkpoint;

import org.apache.nifi.components.DescribedValue;

public enum CheckpointStrategy implements DescribedValue {
    AZURE_BLOB_STORAGE("Azure Blob Storage", "Use Azure Blob Storage to store partition ownership and checkpoint information"),
    COMPONENT_STATE("Component State", "Use component state to store partition ownership and checkpoint information");

    private final String label;
    private final String description;

    CheckpointStrategy(String label, String description) {
        this.label = label;
        this.description = description;
    }

    @Override
    public String getValue() {
        return this.name();
    }

    @Override
    public String getDisplayName() {
        return label;
    }

    @Override
    public String getDescription() {
        return description;
    }
}

