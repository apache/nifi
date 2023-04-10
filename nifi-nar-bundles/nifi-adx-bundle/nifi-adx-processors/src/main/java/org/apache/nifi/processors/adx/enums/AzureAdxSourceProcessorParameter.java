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
package org.apache.nifi.processors.adx.enums;

public enum AzureAdxSourceProcessorParameter {
    DB_NAME("Database name", "The name of the database where the query will be executed."),

    ADX_QUERY("ADX query", "The query which needs to be executed in Azure Data Explorer."),

    ADX_SOURCE_SERVICE("Azure ADX Source Connection Service", "Service that provides the Azure Data Explorer(ADX) Connections.");

    private final String paramDisplayName;

    private final String paramDescription;

    AzureAdxSourceProcessorParameter(String paramDisplayName, String paramDescription) {
        this.paramDisplayName = paramDisplayName;
        this.paramDescription = paramDescription;
    }

    public String getParamDisplayName() {
        return paramDisplayName;
    }

    public String getParamDescription() {
        return paramDescription;
    }
}
