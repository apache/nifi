/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.processors.gcp.vision;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.vision.v1.AsyncBatchAnnotateFilesRequest;
import com.google.cloud.vision.v1.AsyncBatchAnnotateFilesResponse;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.OperationMetadata;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.nifi.processors.gcp.util.GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.gcp.vision.AbstractGcpVisionProcessor.GCP_OPERATION_KEY;
import static org.apache.nifi.processors.gcp.vision.AbstractGcpVisionProcessor.REL_FAILURE;
import static org.apache.nifi.processors.gcp.vision.AbstractGcpVisionProcessor.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StartGcpVisionAnnotateFilesOperationTest {
    private static final Path FLOW_FILE_CONTENT = Paths.get("src/test/resources/vision/annotate-image.json");
    private TestRunner runner = null;

    @Mock
    private OperationFuture<AsyncBatchAnnotateFilesResponse, OperationMetadata> operationFuture;
    @Mock
    private ImageAnnotatorClient mockVisionClient;

    @BeforeEach
    public void setUp() throws InitializationException {
        GCPCredentialsService gcpCredentialsService = new GCPCredentialsControllerService();
        StartGcpVisionAnnotateFilesOperation processor = new StartGcpVisionAnnotateFilesOperation() {
            @Override
            protected ImageAnnotatorClient getVisionClient() {
                return mockVisionClient;
            }
        };
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("gcp-credentials-provider-service-id", gcpCredentialsService);
        runner.enableControllerService(gcpCredentialsService);
        runner.setProperty(GCP_CREDENTIALS_PROVIDER_SERVICE, "gcp-credentials-provider-service-id");
        runner.assertValid(gcpCredentialsService);
        runner.setValidateExpressionUsage(false);
    }

    @Test
    public void testAnnotateFilesJob() throws ExecutionException, InterruptedException, IOException {
        when(mockVisionClient.asyncBatchAnnotateFilesAsync((AsyncBatchAnnotateFilesRequest) any())).thenReturn(operationFuture);
        String operationName = "operationName";
        when(operationFuture.getName()).thenReturn(operationName);
        runner.enqueue(FLOW_FILE_CONTENT, Collections.emptyMap());
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, GCP_OPERATION_KEY);
    }

    @Test
    public void testAnnotateFilesJobFail() throws IOException {
        when(mockVisionClient.asyncBatchAnnotateFilesAsync((AsyncBatchAnnotateFilesRequest) any())).thenThrow(new RuntimeException("ServiceError"));
        runner.enqueue(FLOW_FILE_CONTENT, Collections.emptyMap());
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("json-payload", StartGcpVisionAnnotateFilesOperation.JSON_PAYLOAD.getName()),
                Map.entry("vision-feature-type", AbstractStartGcpVisionOperation.FEATURE_TYPE.getName()),
                Map.entry("output-bucket", AbstractStartGcpVisionOperation.OUTPUT_BUCKET.getName()),
                Map.entry(GoogleUtils.OLD_GCP_CREDENTIALS_PROVIDER_SERVICE_PROPERTY_NAME, GCP_CREDENTIALS_PROVIDER_SERVICE.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }
}
