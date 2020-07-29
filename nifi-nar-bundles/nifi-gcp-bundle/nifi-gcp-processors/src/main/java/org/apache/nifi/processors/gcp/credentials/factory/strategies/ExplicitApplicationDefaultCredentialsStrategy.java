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
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;

import java.io.IOException;
import java.util.Map;


/**
 * Supports GCP Application Default Credentials.  Compared to ImplicitApplicationDefaultCredentialsStrategy, this
 * strategy is designed to be visible to the user, and depends on an affirmative selection from the user.
 *
 * @see <a href="https://developers.google.com/identity/protocols/application-default-credentials">
 *     Application Default Credentials</a>
 */
public class ExplicitApplicationDefaultCredentialsStrategy extends AbstractBooleanCredentialsStrategy {

    public ExplicitApplicationDefaultCredentialsStrategy() {
        super("Application Default Credentials", CredentialPropertyDescriptors.USE_APPLICATION_DEFAULT_CREDENTIALS);
    }

    @Override
    public GoogleCredentials getGoogleCredentials(Map<PropertyDescriptor, String> properties, HttpTransportFactory transportFactory) throws IOException {
        return GoogleCredentials.getApplicationDefault(transportFactory);
    }

}
