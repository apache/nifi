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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

/**
 * Base class for BigQuery Unit Tests. Provides a framework for creating a TestRunner instance with always-required credentials.
 */
@ExtendWith(MockitoExtension.class)
public abstract class AbstractBQTest {
    private static final String PROJECT_ID = System.getProperty("test.gcp.project.id", "nifi-test-gcp-project");
    private static final Integer RETRIES = 9;

    static final String DATASET = RemoteBigQueryHelper.generateDatasetName();

    public static TestRunner buildNewRunner(Processor processor) throws Exception {
        final GCPCredentialsService credentialsService = new GCPCredentialsControllerService();

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("gcpCredentialsControllerService", credentialsService);
        runner.enableControllerService(credentialsService);

        runner.setProperty(AbstractBigQueryProcessor.GCP_CREDENTIALS_PROVIDER_SERVICE, "gcpCredentialsControllerService");
        runner.setProperty(AbstractBigQueryProcessor.PROJECT_ID, PROJECT_ID);
        runner.setProperty(AbstractBigQueryProcessor.RETRY_COUNT, String.valueOf(RETRIES));

        runner.assertValid(credentialsService);

        return runner;
    }

    public abstract AbstractBigQueryProcessor getProcessor();

    protected abstract void addRequiredPropertiesToRunner(TestRunner runner);

    @Mock
    protected BigQuery bq;

    @Test
    public void testBiqQueryOptionsConfiguration() throws Exception {
        reset(bq);
        final TestRunner runner = buildNewRunner(getProcessor());

        final AbstractBigQueryProcessor processor = getProcessor();
        final GoogleCredentials mockCredentials = mock(GoogleCredentials.class);

        final BigQueryOptions options = processor.getServiceOptions(runner.getProcessContext(),
                mockCredentials);

        assertEquals(PROJECT_ID, options.getProjectId(), "Project IDs should match");
        assertEquals(RETRIES.intValue(), options.getRetrySettings().getMaxAttempts(), "Retry counts should match");
        assertSame(mockCredentials, options.getCredentials(), "Credentials should be configured correctly");
    }
}
