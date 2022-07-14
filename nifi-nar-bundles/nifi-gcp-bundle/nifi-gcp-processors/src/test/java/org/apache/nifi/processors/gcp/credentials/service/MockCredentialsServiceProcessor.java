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
package org.apache.nifi.processors.gcp.credentials.service;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Service;
import com.google.cloud.ServiceOptions;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MockCredentialsServiceProcessor extends AbstractGCPProcessor {
    public final List<PropertyDescriptor> properties = Arrays.asList(
            GCP_CREDENTIALS_PROVIDER_SERVICE
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected ServiceOptions getServiceOptions(ProcessContext context, GoogleCredentials credentials) {
        ServiceOptions mockOptions = mock(ServiceOptions.class);
        Service mockService = mock(Service.class);
        when(mockOptions.getService()).thenReturn(mockService);

        return mockOptions;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}
