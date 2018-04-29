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

package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.gcp.GCPIntegrationTests;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNull;

@Category(GCPIntegrationTests.class)
public abstract class AbstractBigQueryIT {

    static final String CONTROLLER_SERVICE = "GCPCredentialsService";
    protected static BigQuery bigquery;
    protected static Dataset dataset;
    protected static TestRunner runner;

    @BeforeClass
    public static void beforeClass() {
        dataset = null;
        BigQueryOptions bigQueryOptions = BigQueryOptions.newBuilder()
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
        final Map<String, String> propertiesMap = new HashMap<>();
        final GCPCredentialsControllerService credentialsControllerService = new GCPCredentialsControllerService();

        runner.addControllerService(CONTROLLER_SERVICE, credentialsControllerService, propertiesMap);
        runner.enableControllerService(credentialsControllerService);
        runner.assertValid(credentialsControllerService);

        return runner;
    }
}
