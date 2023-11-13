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

package org.apache.nifi.processors.aws.ml.translate;

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
import software.amazon.awssdk.services.translate.TranslateClient;
import software.amazon.awssdk.services.translate.model.StartTextTranslationJobRequest;
import software.amazon.awssdk.services.translate.model.StartTextTranslationJobResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_ORIGINAL;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StartAwsTranslateJobTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private TestRunner runner;
    @Mock
    private TranslateClient mockTranslateClient;

    private StartAwsTranslateJob processor;

    private ObjectMapper objectMapper = JsonMapper.builder()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .build();
    @Captor
    private ArgumentCaptor<StartTextTranslationJobRequest> requestCaptor;

    private TestRunner createRunner(final StartAwsTranslateJob processor) {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        AuthUtils.enableAccessKey(runner, "abcd", "defg");
        return runner;
    }

    @BeforeEach
    public void setUp() throws InitializationException {
        processor = new StartAwsTranslateJob() {
            @Override
            public TranslateClient getClient(ProcessContext context) {
                return mockTranslateClient;
            }
        };
        runner = createRunner(processor);
    }

    @Test
    public void testSuccessfulFlowfileContent() throws JsonProcessingException {
        final StartTextTranslationJobRequest request = StartTextTranslationJobRequest.builder()
                .terminologyNames("Name")
                .build();
        final StartTextTranslationJobResponse response = StartTextTranslationJobResponse.builder()
                .jobId(TEST_TASK_ID)
                .build();
        when(mockTranslateClient.startTextTranslationJob(requestCaptor.capture())).thenReturn(response);

        final String requestJson = serialize(request);
        runner.enqueue(requestJson);
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        final String responseData = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next().getContent();
        final StartTextTranslationJobResponse parsedResponse = deserialize(responseData);

        assertEquals(Collections.singletonList("Name"), requestCaptor.getValue().terminologyNames());
        assertEquals(TEST_TASK_ID, parsedResponse.jobId());
    }

    @Test
    public void testSuccessfulAttribute() throws JsonProcessingException {
        final StartTextTranslationJobRequest request = StartTextTranslationJobRequest.builder()
                .terminologyNames("Name")
                .build();
        final StartTextTranslationJobResponse response = StartTextTranslationJobResponse.builder()
                .jobId(TEST_TASK_ID)
                .build();
        when(mockTranslateClient.startTextTranslationJob(requestCaptor.capture())).thenReturn(response);

        final String requestJson = serialize(request);
        runner.setProperty(StartAwsTranslateJob.JSON_PAYLOAD, "${json.payload}");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("json.payload", requestJson);
        runner.enqueue("", attributes);
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        final String responseData = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next().getContent();
        final StartTextTranslationJobResponse parsedResponse = deserialize(responseData);

        assertEquals(Collections.singletonList("Name"), requestCaptor.getValue().terminologyNames());
        assertEquals(TEST_TASK_ID, parsedResponse.jobId());
    }

    @Test
    public void testInvalidJson() {
        final String requestJson = "invalid";
        runner.enqueue(requestJson);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testServiceFailure() throws JsonProcessingException {
        final StartTextTranslationJobRequest request = StartTextTranslationJobRequest.builder()
                .terminologyNames("Name")
                .build();
        when(mockTranslateClient.startTextTranslationJob(requestCaptor.capture())).thenThrow(AwsServiceException.builder().message("message").build());

        final String requestJson = serialize(request);
        runner.enqueue(requestJson);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    private StartTextTranslationJobResponse deserialize(final String responseData) throws JsonProcessingException {
        return objectMapper.readValue(responseData, StartTextTranslationJobResponse.serializableBuilderClass()).build();
    }

    private String serialize(final StartTextTranslationJobRequest request) throws JsonProcessingException {
        return objectMapper.writeValueAsString(request.toBuilder());
    }
}