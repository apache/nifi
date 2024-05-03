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
package org.apache.nifi.services.azure;

import com.azure.core.credential.TokenCredential;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * {@code AzureCredentialsService} interface to support getting a {@code TokenCredential} used for instantiating Azure clients
 */
@Tags({"azure", "security", "credentials", "provider", "session"})
@CapabilityDescription("Provide credentials to use with an Azure clients.")
public interface AzureCredentialsService extends ControllerService {
    /**
     * Get credentials to use with an Azure client
     * @return {@code TokenCredential}
     * @throws ProcessException process exception if there is anything wrong in obtaining the credentials
     *
     * @see <a href="https://learn.microsoft.com/en-us/azure/developer/java/sdk/identity">Azure Identity and Authentication</a>
     */
    TokenCredential getCredentials() throws ProcessException;
}
