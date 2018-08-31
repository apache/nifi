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
package org.apache.nifi.processors.gcp.credentials.factory.strategies;

import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;

import java.io.IOException;
import java.util.Map;

/**
 * Supports Google Compute Engine credentials. Fetches access tokens from the Google Compute Engine metadata server.
 *
 * @see <a href="https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances">
 *     Service Accounts for Instances</a>
 */
public class ComputeEngineCredentialsStrategy extends AbstractBooleanCredentialsStrategy {
    public ComputeEngineCredentialsStrategy() {
        super("Compute Engine Credentials", CredentialPropertyDescriptors.USE_COMPUTE_ENGINE_CREDENTIALS);
    }

    @Override
    public GoogleCredentials getGoogleCredentials(Map<PropertyDescriptor, String> properties, HttpTransportFactory transportFactory) throws IOException {
        return ComputeEngineCredentials.newBuilder()
                .setHttpTransportFactory(transportFactory)
                .build();
    }
}
