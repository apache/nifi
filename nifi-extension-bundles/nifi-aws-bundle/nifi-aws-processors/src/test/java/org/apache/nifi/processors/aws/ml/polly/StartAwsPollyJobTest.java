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

package org.apache.nifi.processors.aws.ml.polly;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStarter;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.processors.aws.v2.AbstractAwsProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.PropertyMigrationResult;
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
import software.amazon.awssdk.services.polly.PollyClient;
import software.amazon.awssdk.services.polly.model.Engine;
import software.amazon.awssdk.services.polly.model.StartSpeechSynthesisTaskRequest;
import software.amazon.awssdk.services.polly.model.StartSpeechSynthesisTaskResponse;
import software.amazon.awssdk.services.polly.model.SynthesisTask;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_ORIGINAL;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StartAwsPollyJobTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private TestRunner runner;
    @Mock
    private PollyClient mockPollyClient;

    private StartAwsPollyJob processor;

    private ObjectMapper objectMapper = JsonMapper.builder()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .build();
    @Captor
    private ArgumentCaptor<StartSpeechSynthesisTaskRequest> requestCaptor;

    private TestRunner createRunner(final StartAwsPollyJob processor) {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        AuthUtils.enableAccessKey(runner, "abcd", "defg");
        return runner;
    }

    @BeforeEach
    public void setUp() throws InitializationException {
        processor = new StartAwsPollyJob() {
            @Override
            public PollyClient getClient(ProcessContext context) {
                return mockPollyClient;
            }
        };
        runner = createRunner(processor);
    }

    @Test
    public void testSuccessfulFlowfileContent() throws JsonProcessingException {
        final StartSpeechSynthesisTaskRequest request = StartSpeechSynthesisTaskRequest.builder()
                .engine(Engine.NEURAL)
                .text("Text")
                .build();
        final StartSpeechSynthesisTaskResponse response = StartSpeechSynthesisTaskResponse.builder()
                .synthesisTask(SynthesisTask.builder().taskId(TEST_TASK_ID).build())
                .build();
        when(mockPollyClient.startSpeechSynthesisTask(requestCaptor.capture())).thenReturn(response);

        final String requestJson = serialize(request);
        runner.enqueue(requestJson);
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        final String responseData = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next().getContent();
        final StartSpeechSynthesisTaskResponse parsedResponse = deserialize(responseData);

        assertEquals("Text", requestCaptor.getValue().text());
        assertEquals(TEST_TASK_ID, parsedResponse.synthesisTask().taskId());
    }

    @Test
    public void testSuccessfulAttribute() throws JsonProcessingException {
        final StartSpeechSynthesisTaskRequest request = StartSpeechSynthesisTaskRequest.builder()
                .engine(Engine.NEURAL)
                .text("Text")
                .build();
        final StartSpeechSynthesisTaskResponse response = StartSpeechSynthesisTaskResponse.builder()
                .synthesisTask(SynthesisTask.builder().taskId(TEST_TASK_ID).build())
                .build();
        when(mockPollyClient.startSpeechSynthesisTask(requestCaptor.capture())).thenReturn(response);

        final String requestJson = serialize(request);
        runner.setProperty(StartAwsPollyJob.JSON_PAYLOAD, "${json.payload}");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("json.payload", requestJson);
        runner.enqueue("", attributes);
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        final String responseData = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next().getContent();
        final StartSpeechSynthesisTaskResponse parsedResponse = deserialize(responseData);

        assertEquals("Text", requestCaptor.getValue().text());
        assertEquals(TEST_TASK_ID, parsedResponse.synthesisTask().taskId());
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
        final StartSpeechSynthesisTaskRequest request = StartSpeechSynthesisTaskRequest.builder()
                .engine(Engine.NEURAL)
                .text("Text")
                .build();
        when(mockPollyClient.startSpeechSynthesisTask(requestCaptor.capture())).thenThrow(AwsServiceException.builder().message("message").build());

        final String requestJson = serialize(request);
        runner.enqueue(requestJson);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    void testMigration() {
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expected = Map.of("aws-region", AbstractAwsProcessor.REGION.getName(),
                "json-payload", AbstractAwsMachineLearningJobStarter.JSON_PAYLOAD.getName());

        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());
    }

    private StartSpeechSynthesisTaskResponse deserialize(final String responseData) throws JsonProcessingException {
        return objectMapper.readValue(responseData, StartSpeechSynthesisTaskResponse.serializableBuilderClass()).build();
    }

    private String serialize(final StartSpeechSynthesisTaskRequest request) throws JsonProcessingException {
        return objectMapper.writeValueAsString(request.toBuilder());
    }
}