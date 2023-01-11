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
import static org.apache.nifi.processors.gcp.vision.AbstractGetGcpVisionAnnotateOperationStatus.REL_ORIGINAL;
import static org.mockito.Mockito.when;

import com.google.cloud.vision.v1.AsyncBatchAnnotateImagesResponse;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsClient;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import java.util.Collections;
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
public class GetGcpVisionAnnotateImagesOperationStatusTest {
    private static final String PLACEHOLDER_CONTENT = "content";
    private static final String OPERATION_KEY = "operationKey";
    private TestRunner runner = null;
    private GetGcpVisionAnnotateImagesOperationStatus processor;
    @Mock
    private ImageAnnotatorClient mockVisionClient;
    private GCPCredentialsService gcpCredentialsService;
    @Mock
    private OperationsClient operationClient;
    @Mock
    private Operation operation;

    @BeforeEach
    public void setUp() throws InitializationException {
        gcpCredentialsService = new GCPCredentialsControllerService();
        processor = new GetGcpVisionAnnotateImagesOperationStatus() {
            @Override
            protected ImageAnnotatorClient getVisionClient() {
                return mockVisionClient;
            }

            @Override
            protected GeneratedMessageV3 deserializeResponse(ByteString responseValue) throws InvalidProtocolBufferException {
                return AsyncBatchAnnotateImagesResponse.newBuilder().build();
            }
        };
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("gcp-credentials-provider-service-id", gcpCredentialsService);
        runner.enableControllerService(gcpCredentialsService);
        runner.setProperty(GCP_CREDENTIALS_PROVIDER_SERVICE, "gcp-credentials-provider-service-id");
        runner.assertValid(gcpCredentialsService);
    }

    @Test
    public void testGetAnnotateImagesJobStatusSuccess() {
        when(mockVisionClient.getOperationsClient()).thenReturn(operationClient);
        when(operationClient.getOperation(OPERATION_KEY)).thenReturn(operation);
        when(operation.getResponse()).thenReturn(Any.newBuilder().build());
        when(operation.getDone()).thenReturn(true);
        when(operation.hasError()).thenReturn(false);
        runner.enqueue(PLACEHOLDER_CONTENT, Collections.singletonMap(GCP_OPERATION_KEY, OPERATION_KEY));
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
    }

    @Test
    public void testGetAnnotateImagesJobStatusInProgress() {
        when(mockVisionClient.getOperationsClient()).thenReturn(operationClient);
        when(operationClient.getOperation(OPERATION_KEY)).thenReturn(operation);
        when(operation.getDone()).thenReturn(true);
        when(operation.hasError()).thenReturn(true);
        runner.enqueue(PLACEHOLDER_CONTENT, Collections.singletonMap(GCP_OPERATION_KEY, OPERATION_KEY));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testGetAnnotateImagesJobStatusFailed() {
        when(mockVisionClient.getOperationsClient()).thenReturn(operationClient);
        when(operationClient.getOperation(OPERATION_KEY)).thenReturn(operation);
        when(operation.getDone()).thenReturn(true);
        when(operation.hasError()).thenReturn(true);
        when(operation.getError()).thenReturn(Status.newBuilder().build());
        runner.enqueue(PLACEHOLDER_CONTENT, Collections.singletonMap(GCP_OPERATION_KEY, OPERATION_KEY));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }
}