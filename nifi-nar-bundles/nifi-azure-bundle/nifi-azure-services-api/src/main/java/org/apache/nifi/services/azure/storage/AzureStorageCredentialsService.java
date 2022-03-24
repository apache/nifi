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
 * AzureStorageCredentialsService interface to support getting Storage Account Name and Storage Credentials
 * used for instantiating Azure Storage clients.
 */
public interface AzureStorageCredentialsService extends ControllerService {

    /**
     * Get AzureStorageCredentialsDetails object which contains the Storage Account Name and the Storage Credentials
     * @param attributes FlowFile attributes (typically)
     * @return AzureStorageCredentialsDetails object
     */
    AzureStorageCredentialsDetails getStorageCredentialsDetails(Map<String, String> attributes);
}
