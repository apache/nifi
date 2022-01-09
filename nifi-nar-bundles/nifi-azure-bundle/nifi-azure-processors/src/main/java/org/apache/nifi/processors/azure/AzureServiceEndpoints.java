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
package org.apache.nifi.processors.azure;

public class AzureServiceEndpoints {

    private static final String DEFAULT_STORAGE_ENDPOINT_SUFFIX = ".core.windows.net";
    public static final String DEFAULT_BLOB_ENDPOINT_SUFFIX = "blob" + DEFAULT_STORAGE_ENDPOINT_SUFFIX;
    public static final String DEFAULT_ADLS_ENDPOINT_SUFFIX = "dfs" + DEFAULT_STORAGE_ENDPOINT_SUFFIX;
    public static final String DEFAULT_QUEUE_ENDPOINT_SUFFIX = "queue" + DEFAULT_STORAGE_ENDPOINT_SUFFIX;

    private AzureServiceEndpoints() {
    }

    public static String getAzureBlobStorageEndpoint(String accountName, String endpointSuffix) {
        return String.format("https://%s.%s", accountName, endpointSuffix != null ? endpointSuffix : DEFAULT_BLOB_ENDPOINT_SUFFIX);
    }

    public static String getAzureDataLakeStorageEndpoint(String accountName, String endpointSuffix) {
        return String.format("https://%s.%s", accountName, endpointSuffix != null ? endpointSuffix : DEFAULT_ADLS_ENDPOINT_SUFFIX);
    }
}
