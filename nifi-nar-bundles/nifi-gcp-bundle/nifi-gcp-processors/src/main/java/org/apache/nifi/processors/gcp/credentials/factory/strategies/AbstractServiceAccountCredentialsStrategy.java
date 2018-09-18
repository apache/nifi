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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Abstract class handling any of the service account related credential strategies, whether provided directly to NiFi
 * or through a flat JSON file.
 */
public abstract class AbstractServiceAccountCredentialsStrategy extends AbstractCredentialsStrategy {
    public AbstractServiceAccountCredentialsStrategy(String name, PropertyDescriptor[] requiredProperties) {
        super(name, requiredProperties);
    }

    protected abstract InputStream getServiceAccountJson(Map<PropertyDescriptor, String> properties) throws IOException;

    @Override
    public GoogleCredentials getGoogleCredentials(Map<PropertyDescriptor, String> properties, HttpTransportFactory transportFactory) throws IOException {
            return GoogleCredentials.fromStream(getServiceAccountJson(properties), transportFactory);
    }

}
