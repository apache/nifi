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
package org.apache.nifi.adx;

public enum AzureDataExplorerParameter {

    AUTH_STRATEGY("Kusto Authentication Method", "The strategy or method to authenticate against Azure Data Explorer"),
    APP_ID("Application ID", "Azure application ID for accessing the ADX-Cluster"),
    APP_KEY("Application KEY", "Azure application Key for accessing the ADX-Cluster"),
    APP_TENANT("Application Tenant", "Azure application tenant for accessing the ADX-Cluster"),
    CLUSTER_URL("Cluster URL", "Endpoint of ADX cluster. This is required only when streaming data to ADX cluster is enabled.");

    private final String paramDisplayName;
    private final String description;


    AzureDataExplorerParameter(String paramDisplayName, String description) {
        this.paramDisplayName = paramDisplayName;
        this.description = description;
    }

    public String getDisplayName() {
        return paramDisplayName;
    }

    public String getDescription() {
        return description;
    }
}
