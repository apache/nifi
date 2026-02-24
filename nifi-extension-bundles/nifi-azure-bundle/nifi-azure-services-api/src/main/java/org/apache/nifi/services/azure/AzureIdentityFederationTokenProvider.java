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
import org.apache.nifi.controller.ControllerService;

/**
 * Controller Service that provides Azure {@link TokenCredential} for workload identity federation.
 * Implementations exchange an external identity token for an Azure AD access token suitable for
 * Azure service clients (for example, Storage, Data Lake, or Event Hubs).
 */
public interface AzureIdentityFederationTokenProvider extends ControllerService {

    /**
     * Returns a {@link TokenCredential} that can be used to authenticate with Azure services.
     * The credential handles token acquisition and refresh automatically.
     *
     * @return a TokenCredential for Azure service authentication
     */
    TokenCredential getCredentials();
}
