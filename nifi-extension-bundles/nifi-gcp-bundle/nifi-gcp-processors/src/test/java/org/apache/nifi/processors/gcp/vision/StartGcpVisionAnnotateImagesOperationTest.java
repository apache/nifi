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
import static org.apache.nifi.processors.gcp.vision.AbstractGcpVisionProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.gcp.vision.StartGcpVisionAnnotateImagesOperation.JSON_PAYLOAD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StartGcpVisionAnnotateImagesOperationTest {
    private TestRunner runner = null;
    private final String operationName = "operationName";
    @Mock
    private OperationFuture operationFuture;
    @Mock
    private ApiFuture<OperationSnapshot> apiFuture;
    @Mock
    private ImageAnnotatorClient mockVisionClient;
    @Mock
    private OperationSnapshot operationSnapshot;

    @BeforeEach
    public void setUp() throws InitializationException, IOException {
        String jsonPayloadValue = FileUtils.readFileToString(new File("src/test/resources/vision/annotate-image.json"), StandardCharsets.UTF_8);
        GCPCredentialsService gcpCredentialsService = new GCPCredentialsControllerService();
        StartGcpVisionAnnotateImagesOperation processor = new StartGcpVisionAnnotateImagesOperation() {
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
        runner.setProperty(JSON_PAYLOAD, jsonPayloadValue);
        runner.setValidateExpressionUsage(false);
    }

    @Test
    public void testAnnotateImageJob() throws ExecutionException, InterruptedException {
        when(mockVisionClient.asyncBatchAnnotateImagesAsync(any())).thenReturn(operationFuture);
        when(operationFuture.getName()).thenReturn(operationName);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, GCP_OPERATION_KEY);
    }

    @Test
    public void testAnnotateFilesJob() throws ExecutionException, InterruptedException {
        when(mockVisionClient.asyncBatchAnnotateImagesAsync(any())).thenReturn(operationFuture);
        when(operationFuture.getName()).thenReturn(operationName);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, GCP_OPERATION_KEY);
    }
}