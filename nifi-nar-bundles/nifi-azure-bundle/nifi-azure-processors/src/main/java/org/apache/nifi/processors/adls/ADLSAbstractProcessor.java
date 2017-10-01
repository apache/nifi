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
package org.apache.nifi.processors.adls;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.ADLStoreOptions;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.processors.adls.ADLSConstants.ACCOUNT_NAME;
import static org.apache.nifi.processors.adls.ADLSConstants.REL_FAILURE;
import static org.apache.nifi.processors.adls.ADLSConstants.REL_SUCCESS;
import static org.apache.nifi.processors.adls.ADLSConstants.CLIENT_ID;
import static org.apache.nifi.processors.adls.ADLSConstants.CLIENT_SECRET;
import static org.apache.nifi.processors.adls.ADLSConstants.AUTH_TOKEN_ENDPOINT;

public abstract class ADLSAbstractProcessor extends AbstractProcessor {

    protected List<PropertyDescriptor> descriptors;

    protected Set<Relationship> relationships;

    protected volatile ADLStoreClient adlStoreClient;

    protected final static String ADLS_FILE_SEPARATOR = "/";
    private String accountFQDN;

    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = relationships;

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ACCOUNT_NAME);
        properties.add(CLIENT_ID);
        properties.add(CLIENT_SECRET);
        properties.add(AUTH_TOKEN_ENDPOINT);
        this.descriptors = properties;
    }

    @OnScheduled
    protected void onScheduled(ProcessContext context) {
        createADLSClient(context);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    protected ADLStoreClient createADLSClientUnsafe(final ProcessContext context) {

        final String adlsStoreClientUserAgent = "ADLNIFIProcessor/1.0";
        final String clientId = context.getProperty(CLIENT_ID).getValue();
        final String clientSecret = context.getProperty(CLIENT_SECRET).getValue();
        final String authTokenEndPoint = context.getProperty(AUTH_TOKEN_ENDPOINT).getValue();
        final String accountName = context.getProperty(ACCOUNT_NAME).getValue();
        accountFQDN = accountName;

        final ClientCredsTokenProvider clientCredsTokenProvider =
                new ClientCredsTokenProvider(authTokenEndPoint, clientId, clientSecret);
        ADLStoreClient adlStoreClient = ADLStoreClient.createClient(accountFQDN, clientCredsTokenProvider);
        try {
            adlStoreClient.setOptions(new ADLStoreOptions().setUserAgentSuffix(adlsStoreClientUserAgent));
        } catch (IOException e) {
            getLogger().debug("ADLS client user agent not set: {}", new Object[] {e});
        }
        return adlStoreClient;
    }

    protected void createADLSClient(ProcessContext context) {
        if(adlStoreClient == null) {
            synchronized (this) {
                if(adlStoreClient == null) {
                    adlStoreClient = createADLSClientUnsafe(context);
                }
            }
        }
    }

    public ADLStoreClient getAdlStoreClient(ProcessContext context) {
        if(adlStoreClient == null)
            createADLSClient(context);
        return adlStoreClient;
    }

    public String getTransitURI(String filePath) {
        return "adl://"+accountFQDN+"/"+filePath;
    }
}
