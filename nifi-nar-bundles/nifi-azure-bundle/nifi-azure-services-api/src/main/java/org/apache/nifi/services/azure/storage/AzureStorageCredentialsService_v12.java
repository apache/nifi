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

import org.apache.nifi.controller.ControllerService;

import java.util.Map;

/**
 * Service interface to provide Azure credentials details for processors using Azure Storage Java v12 client library.
 */
public interface AzureStorageCredentialsService_v12 extends ControllerService {

    /**
     * Get AzureStorageCredentialsDetails_v12 object which contains the Storage Account Name, the Storage Service Endpoint Suffix and the parameters of the Storage Credentials
     * @param attributes FlowFile attributes (typically)
     * @return AzureStorageCredentialsDetails_v12 object
     */
    AzureStorageCredentialsDetails_v12 getCredentialsDetails(Map<String, String> attributes);
}
