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

package org.apache.nifi.processors.aws.ml.textract;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
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
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.model.StartDocumentAnalysisRequest;
import software.amazon.awssdk.services.textract.model.StartDocumentAnalysisResponse;
import software.amazon.awssdk.services.textract.model.StartDocumentTextDetectionRequest;
import software.amazon.awssdk.services.textract.model.StartDocumentTextDetectionResponse;
import software.amazon.awssdk.services.textract.model.StartExpenseAnalysisRequest;
import software.amazon.awssdk.services.textract.model.StartExpenseAnalysisResponse;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_ORIGINAL;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.textract.TextractType.DOCUMENT_ANALYSIS;
import static org.apache.nifi.processors.aws.ml.textract.TextractType.DOCUMENT_TEXT_DETECTION;
import static org.apache.nifi.processors.aws.ml.textract.TextractType.EXPENSE_ANALYSIS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StartAwsTextractJobStatusTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private TestRunner runner;
    @Mock
    private TextractClient mockTextractClient;

    private StartAwsTextractJob processor;

    private ObjectMapper objectMapper = JsonMapper.builder()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .build();
    @Captor
    private ArgumentCaptor<StartDocumentAnalysisRequest> documentAnalysisCaptor;
    @Captor
    private ArgumentCaptor<StartExpenseAnalysisRequest> expenseAnalysisRequestCaptor;
    @Captor
    private ArgumentCaptor<StartDocumentTextDetectionRequest> documentTextDetectionCaptor;

    private TestRunner createRunner(final StartAwsTextractJob processor) {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        AuthUtils.enableAccessKey(runner, "abcd", "defg");
        return runner;
    }

    @BeforeEach
    public void setUp() throws InitializationException {
        processor = new StartAwsTextractJob() {
            @Override
            public TextractClient getClient(ProcessContext context) {
                return mockTextractClient;
            }
        };
        runner = createRunner(processor);
    }

    @Test
    public void testSuccessfulDocumentAnalysisFlowfileContent() throws JsonProcessingException {
        final StartDocumentAnalysisRequest request = StartDocumentAnalysisRequest.builder()
                .jobTag("Tag")
                .build();
        final StartDocumentAnalysisResponse response = StartDocumentAnalysisResponse.builder()
                .jobId(TEST_TASK_ID)
                .build();
        when(mockTextractClient.startDocumentAnalysis(documentAnalysisCaptor.capture())).thenReturn(response);

        final String requestJson = serialize(request);
        runner.setProperty(StartAwsTextractJob.TEXTRACT_TYPE, DOCUMENT_ANALYSIS.getType());
        runner.enqueue(requestJson);
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        final String responseData = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next().getContent();
        final StartDocumentAnalysisResponse parsedResponse = deserializeDARequest(responseData);

        assertEquals("Tag", documentAnalysisCaptor.getValue().jobTag());
        assertEquals(TEST_TASK_ID, parsedResponse.jobId());
    }

    @Test
    public void testSuccessfulDocumentAnalysisAttribute() throws JsonProcessingException {
        final StartDocumentAnalysisRequest request = StartDocumentAnalysisRequest.builder()
                .jobTag("Tag")
                .build();
        final StartDocumentAnalysisResponse response = StartDocumentAnalysisResponse.builder()
                .jobId(TEST_TASK_ID)
                .build();
        when(mockTextractClient.startDocumentAnalysis(documentAnalysisCaptor.capture())).thenReturn(response);

        final String requestJson = serialize(request);
        runner.setProperty(StartAwsTextractJob.TEXTRACT_TYPE, DOCUMENT_ANALYSIS.getType());
        runner.setProperty(StartAwsTextractJob.JSON_PAYLOAD, "${json.payload}");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("json.payload", requestJson);
        runner.enqueue("", attributes);
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        final String responseData = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next().getContent();
        final StartDocumentAnalysisResponse parsedResponse = deserializeDARequest(responseData);

        assertEquals("Tag", documentAnalysisCaptor.getValue().jobTag());
        assertEquals(TEST_TASK_ID, parsedResponse.jobId());
    }

    @Test
    public void testInvalidDocumentAnalysisJson() {
        final String requestJson = "invalid";
        runner.enqueue(requestJson);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testDocumentAnalysisServiceFailure() throws JsonProcessingException {
        final StartDocumentAnalysisRequest request = StartDocumentAnalysisRequest.builder()
                .jobTag("Tag")
                .build();
        when(mockTextractClient.startDocumentAnalysis(documentAnalysisCaptor.capture())).thenThrow(AwsServiceException.builder().message("message").build());

        final String requestJson = serialize(request);
        runner.enqueue(requestJson);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testSuccessfulExpenseAnalysisFlowfileContent() throws JsonProcessingException {
        final StartExpenseAnalysisRequest request = StartExpenseAnalysisRequest.builder()
                .jobTag("Tag")
                .build();
        final StartExpenseAnalysisResponse response = StartExpenseAnalysisResponse.builder()
                .jobId(TEST_TASK_ID)
                .build();
        when(mockTextractClient.startExpenseAnalysis(expenseAnalysisRequestCaptor.capture())).thenReturn(response);

        final String requestJson = serialize(request);
        runner.setProperty(StartAwsTextractJob.TEXTRACT_TYPE, EXPENSE_ANALYSIS.getType());
        runner.enqueue(requestJson);
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        final String responseData = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next().getContent();
        final StartExpenseAnalysisResponse parsedResponse = deserializeEARequest(responseData);

        assertEquals("Tag", expenseAnalysisRequestCaptor.getValue().jobTag());
        assertEquals(TEST_TASK_ID, parsedResponse.jobId());
    }

    @Test
    public void testSuccessfulExpenseAnalysisAttribute() throws JsonProcessingException {
        final StartExpenseAnalysisRequest request = StartExpenseAnalysisRequest.builder()
                .jobTag("Tag")
                .build();
        final StartExpenseAnalysisResponse response = StartExpenseAnalysisResponse.builder()
                .jobId(TEST_TASK_ID)
                .build();
        when(mockTextractClient.startExpenseAnalysis(expenseAnalysisRequestCaptor.capture())).thenReturn(response);

        final String requestJson = serialize(request);
        runner.setProperty(StartAwsTextractJob.TEXTRACT_TYPE, EXPENSE_ANALYSIS.getType());
        runner.setProperty(StartAwsTextractJob.JSON_PAYLOAD, "${json.payload}");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("json.payload", requestJson);
        runner.enqueue("", attributes);
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        final String responseData = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next().getContent();
        final StartExpenseAnalysisResponse parsedResponse = deserializeEARequest(responseData);

        assertEquals("Tag", expenseAnalysisRequestCaptor.getValue().jobTag());
        assertEquals(TEST_TASK_ID, parsedResponse.jobId());
    }

    @Test
    public void testInvalidExpenseAnalysisJson() {
        final String requestJson = "invalid";
        runner.setProperty(StartAwsTextractJob.TEXTRACT_TYPE, EXPENSE_ANALYSIS.getType());
        runner.enqueue(requestJson);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testExpenseAnalysisServiceFailure() throws JsonProcessingException {
        final StartExpenseAnalysisRequest request = StartExpenseAnalysisRequest.builder()
                .jobTag("Tag")
                .build();
        when(mockTextractClient.startExpenseAnalysis(expenseAnalysisRequestCaptor.capture())).thenThrow(AwsServiceException.builder().message("message").build());

        final String requestJson = serialize(request);
        runner.setProperty(StartAwsTextractJob.TEXTRACT_TYPE, EXPENSE_ANALYSIS.getType());
        runner.enqueue(requestJson);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testSuccessfulDocumentTextDetectionFlowfileContent() throws JsonProcessingException {
        final StartDocumentTextDetectionRequest request = StartDocumentTextDetectionRequest.builder()
                .jobTag("Tag")
                .build();
        final StartDocumentTextDetectionResponse response = StartDocumentTextDetectionResponse.builder()
                .jobId(TEST_TASK_ID)
                .build();
        when(mockTextractClient.startDocumentTextDetection(documentTextDetectionCaptor.capture())).thenReturn(response);

        final String requestJson = serialize(request);
        runner.setProperty(StartAwsTextractJob.TEXTRACT_TYPE, DOCUMENT_TEXT_DETECTION.getType());
        runner.enqueue(requestJson);
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        final String responseData = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next().getContent();
        final StartDocumentTextDetectionResponse parsedResponse = deserializeDTDRequest(responseData);

        assertEquals("Tag", documentTextDetectionCaptor.getValue().jobTag());
        assertEquals(TEST_TASK_ID, parsedResponse.jobId());
    }

    @Test
    public void testSuccessfulDocumentTextDetectionAttribute() throws JsonProcessingException {
        final StartDocumentTextDetectionRequest request = StartDocumentTextDetectionRequest.builder()
                .jobTag("Tag")
                .build();
        final StartDocumentTextDetectionResponse response = StartDocumentTextDetectionResponse.builder()
                .jobId(TEST_TASK_ID)
                .build();
        when(mockTextractClient.startDocumentTextDetection(documentTextDetectionCaptor.capture())).thenReturn(response);

        final String requestJson = serialize(request);
        runner.setProperty(StartAwsTextractJob.TEXTRACT_TYPE, DOCUMENT_TEXT_DETECTION.getType());
        runner.setProperty(StartAwsTextractJob.JSON_PAYLOAD, "${json.payload}");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("json.payload", requestJson);
        runner.enqueue("", attributes);
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        final String responseData = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next().getContent();
        final StartDocumentTextDetectionResponse parsedResponse = deserializeDTDRequest(responseData);

        assertEquals("Tag", documentTextDetectionCaptor.getValue().jobTag());
        assertEquals(TEST_TASK_ID, parsedResponse.jobId());
    }

    @Test
    public void testInvalidDocumentTextDetectionJson() {
        final String requestJson = "invalid";
        runner.enqueue(requestJson);
        runner.setProperty(StartAwsTextractJob.TEXTRACT_TYPE, DOCUMENT_TEXT_DETECTION.getType());
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testDocumentTextDetectionServiceFailure() throws JsonProcessingException {
        final StartDocumentTextDetectionRequest request = StartDocumentTextDetectionRequest.builder()
                .jobTag("Tag")
                .build();
        when(mockTextractClient.startDocumentTextDetection(documentTextDetectionCaptor.capture())).thenThrow(AwsServiceException.builder().message("message").build());

        final String requestJson = serialize(request);
        runner.enqueue(requestJson);
        runner.setProperty(StartAwsTextractJob.TEXTRACT_TYPE, DOCUMENT_TEXT_DETECTION.getType());
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    private StartDocumentTextDetectionResponse deserializeDTDRequest(final String responseData) throws JsonProcessingException {
        return objectMapper.readValue(responseData, StartDocumentTextDetectionResponse.serializableBuilderClass()).build();
    }

    private StartDocumentAnalysisResponse deserializeDARequest(final String responseData) throws JsonProcessingException {
        return objectMapper.readValue(responseData, StartDocumentAnalysisResponse.serializableBuilderClass()).build();
    }

    private StartExpenseAnalysisResponse deserializeEARequest(final String responseData) throws JsonProcessingException {
        return objectMapper.readValue(responseData, StartExpenseAnalysisResponse.serializableBuilderClass()).build();
    }

    private String serialize(final StartDocumentAnalysisRequest request) throws JsonProcessingException {
        return objectMapper.writeValueAsString(request.toBuilder());
    }

    private String serialize(final StartExpenseAnalysisRequest request) throws JsonProcessingException {
        return objectMapper.writeValueAsString(request.toBuilder());
    }

    private String serialize(final StartDocumentTextDetectionRequest request) throws JsonProcessingException {
        return objectMapper.writeValueAsString(request.toBuilder());
    }
}