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
package org.apache.nifi.services.azure.eventhub;

import org.apache.nifi.components.DescribedValue;

public enum AzureAuthenticationStrategy implements DescribedValue {
    SHARED_ACCESS_KEY("Shared Access Key", "Azure Event Hub shared access key"),
    DEFAULT_AZURE_CREDENTIAL("Default Azure Credential", "The Default Azure Credential " +
            "will read credentials from standard environment variables and will also attempt to read " +
            "Managed Identity credentials when running in Microsoft Azure environments");

    private final String displayName;
    private final String description;

    AzureAuthenticationStrategy(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getValue() {
        return name();
    }
}
