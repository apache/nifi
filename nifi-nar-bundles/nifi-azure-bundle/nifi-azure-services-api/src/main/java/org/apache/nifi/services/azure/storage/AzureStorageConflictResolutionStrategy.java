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
package org.apache.nifi.services.azure.storage;

import org.apache.nifi.components.DescribedValue;

public enum AzureStorageConflictResolutionStrategy implements DescribedValue {
    FAIL_RESOLUTION("fail", "Fail if the blob already exists"),
    IGNORE_RESOLUTION("ignore", "Ignore if the blob already exists; the 'azure.error' attribute will be set to the value 'BLOB_ALREADY_EXISTS'"),
    REPLACE_RESOLUTION("replace", "Replace blob contents if the blob already exist");

    private final String label;
    private final String description;

    AzureStorageConflictResolutionStrategy(String label, String description) {
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
