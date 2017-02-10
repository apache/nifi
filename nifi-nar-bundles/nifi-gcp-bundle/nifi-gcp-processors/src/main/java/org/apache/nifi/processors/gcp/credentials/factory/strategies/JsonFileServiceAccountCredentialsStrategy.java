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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;


/**
 * Supports service account credentials in a JSON file.
 *
 * @see <a href="https://cloud.google.com/iam/docs/service-accounts">
 *     Service Accounts</a>
 */
public class JsonFileServiceAccountCredentialsStrategy extends AbstractServiceAccountCredentialsStrategy {

    public JsonFileServiceAccountCredentialsStrategy() {
        super("Service Account Credentials (Json File)", new PropertyDescriptor[] {
                CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE
        });
    }

    @Override
    protected InputStream getServiceAccountJson(Map<PropertyDescriptor, String> properties) throws IOException {
        String serviceAccountFile = properties.get(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE);
        return new BufferedInputStream(Files.newInputStream(Paths.get(serviceAccountFile)));
    }
}
