/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.gcp.bigquery;

import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.gcp.GCPIntegrationTests;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialsFactory;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;

@Category(GCPIntegrationTests.class)
public abstract class AbstractBigQueryIT {

    protected static final String CONTROLLER_SERVICE = "GCPCredentialsService";
    protected static final String PROJECT_ID = System.getProperty("test.gcp.project.id", "nifi");
    protected static final String SERVICE_ACCOUNT_JSON = System.getProperty("test.gcp.service.account", "/path/to/service/account.json");

    protected static BigQuery bigquery;
    protected static Dataset dataset;
    protected static TestRunner runner;

    private static final CredentialsFactory credentialsProviderFactory = new CredentialsFactory();

    @BeforeClass
    public static void beforeClass() throws IOException {
        final Map<PropertyDescriptor, String> propertiesMap = new HashMap<>();
        propertiesMap.put(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE, SERVICE_ACCOUNT_JSON);
        Credentials credentials = credentialsProviderFactory.getGoogleCredentials(propertiesMap, new ProxyAwareTransportFactory(null));

        BigQueryOptions bigQueryOptions = BigQueryOptions.newBuilder()
                .setProjectId(PROJECT_ID)
                .setCredentials(credentials)
                .build();

        bigquery = bigQueryOptions.getService();

        DatasetInfo datasetInfo = DatasetInfo.newBuilder(RemoteBigQueryHelper.generateDatasetName()).build();
        dataset = bigquery.create(datasetInfo);
    }

    @AfterClass
    public static void afterClass() {
        bigquery.delete(dataset.getDatasetId(), BigQuery.DatasetDeleteOption.deleteContents());
    }

    protected static void validateNoServiceExceptionAttribute(FlowFile flowFile) {
        assertNull(flowFile.getAttribute(BigQueryAttributes.JOB_ERROR_MSG_ATTR));
        assertNull(flowFile.getAttribute(BigQueryAttributes.JOB_ERROR_REASON_ATTR));
        assertNull(flowFile.getAttribute(BigQueryAttributes.JOB_ERROR_LOCATION_ATTR));
    }

    protected TestRunner setCredentialsControllerService(TestRunner runner) throws InitializationException {
        final GCPCredentialsControllerService credentialsControllerService = new GCPCredentialsControllerService();

        final Map<String, String> propertiesMap = new HashMap<>();
        propertiesMap.put(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE.getName(), SERVICE_ACCOUNT_JSON);

        runner.addControllerService(CONTROLLER_SERVICE, credentialsControllerService, propertiesMap);
        runner.enableControllerService(credentialsControllerService);
        runner.assertValid(credentialsControllerService);

        return runner;
    }
}
