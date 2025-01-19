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
package org.apache.nifi.processors.azure;

import com.azure.storage.file.datalake.DataLakeServiceClient;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.DataLakeServiceClientFactory;
import org.apache.nifi.services.azure.storage.ADLSCredentialsDetails;
import org.apache.nifi.services.azure.storage.ADLSCredentialsService;

import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.ADLS_CREDENTIALS_SERVICE;

public abstract class AbstractAzureDataLakeStorageProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to Azure storage are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to Azure storage for some reason are transferred to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    public static final String TEMP_FILE_DIRECTORY = "_nifitempdirectory";

    private volatile DataLakeServiceClientFactory clientFactory;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientFactory = new DataLakeServiceClientFactory(getLogger(), AzureStorageUtils.getProxyOptions(context));
    }

    @OnStopped
    public void onStopped() {
        clientFactory = null;
    }

    public DataLakeServiceClient getStorageClient(PropertyContext context, FlowFile flowFile) {
        final Map<String, String> attributes = flowFile != null ? flowFile.getAttributes() : Map.of();

        final ADLSCredentialsService credentialsService = context.getProperty(ADLS_CREDENTIALS_SERVICE)
                .asControllerService(ADLSCredentialsService.class);
        final ADLSCredentialsDetails credentialsDetails = credentialsService.getCredentialsDetails(attributes);

        return clientFactory.getStorageClient(credentialsDetails);
    }
}
