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
package org.apache.nifi.processors.gcp.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;

public class GoogleUtils {

    public static final String GOOGLE_CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

    /**
     * Links to the {@link GCPCredentialsService} which provides credentials for this particular processor.
     */
    public static final PropertyDescriptor GCP_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("gcp-credentials-provider-service")
            .displayName("GCP Credentials Provider Service")
            .description("The Controller Service used to obtain Google Cloud Platform credentials.")
            .required(true)
            .identifiesControllerService(GCPCredentialsService.class)
            .build();
}
