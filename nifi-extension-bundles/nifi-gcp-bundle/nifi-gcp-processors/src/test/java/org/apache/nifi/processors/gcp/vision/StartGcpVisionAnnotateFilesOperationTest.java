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

import static org.apache.nifi.processors.gcp.util.GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.gcp.vision.AbstractGcpVisionProcessor.GCP_OPERATION_KEY;
import static org.apache.nifi.processors.gcp.vision.AbstractGcpVisionProcessor.REL_FAILURE;
import static org.apache.nifi.processors.gcp.vision.AbstractGcpVisionProcessor.REL_SUCCESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.cloud.vision.v1.AsyncBatchAnnotateFilesRequest;
import com.google.cloud.vision.v1.AsyncBatchAnnotateFilesResponse;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.OperationMetadata;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StartGcpVisionAnnotateFilesOperationTest {
    private TestRunner runner = null;
    private static final Path FlowFileContent = Paths.get("src/test/resources/vision/annotate-image.json");
    @Mock
    private ImageAnnotatorClient vision;
    @Captor
    private ArgumentCaptor<AsyncBatchAnnotateFilesRequest> requestCaptor;
    private String operationName = "operationName";
    @Mock
    private OperationFuture<AsyncBatchAnnotateFilesResponse, OperationMetadata> operationFuture;
    @Mock
    private ApiFuture<OperationSnapshot> apiFuture;
    @Mock
    private ImageAnnotatorClient mockVisionClient;
    private GCPCredentialsService gcpCredentialsService;
    @Mock
    private OperationSnapshot operationSnapshot;

    @BeforeEach
    public void setUp() throws InitializationException {
        gcpCredentialsService = new GCPCredentialsControllerService();
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
        when(operationFuture.getName()).thenReturn(operationName);
        runner.enqueue(FlowFileContent, Collections.emptyMap());
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, GCP_OPERATION_KEY);
    }

    @Test
    public void testAnnotateFilesJobFail() throws IOException {
        when(mockVisionClient.asyncBatchAnnotateFilesAsync((AsyncBatchAnnotateFilesRequest) any())).thenThrow(new RuntimeException("ServiceError"));
        runner.enqueue(FlowFileContent, Collections.emptyMap());
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE);
    }
}