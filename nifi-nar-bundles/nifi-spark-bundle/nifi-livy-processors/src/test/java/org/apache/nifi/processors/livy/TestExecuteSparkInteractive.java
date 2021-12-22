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
package org.apache.nifi.processors.livy;

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.nifi.controller.api.livy.LivySessionService;
import org.apache.nifi.controller.api.livy.exception.SessionManagerException;
import org.apache.nifi.controller.livy.LivySessionController;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestExecuteSparkInteractive {
    @Mock
    private LivySessionService livySessionService;

    @Mock
    private HttpClient httpClient;

    private TestRunner runner;

    @BeforeEach
    public void before() throws Exception {
        final String identifier = LivySessionController.class.getSimpleName();
        runner = TestRunners.newTestRunner(ExecuteSparkInteractive.class);

        when(livySessionService.getIdentifier()).thenReturn(identifier);
        runner.addControllerService(identifier, livySessionService);
        runner.enableControllerService(livySessionService);
        runner.setProperty(ExecuteSparkInteractive.LIVY_CONTROLLER_SERVICE, identifier);
    }

    @AfterEach
    public void after() {
        runner.shutdown();
    }

    @Test
    public void testSparkSession() throws SessionManagerException, IOException {
        testCode("print \"hello world\"");
    }

    @Test
    public void testSparkSessionWithSpecialChars() throws SessionManagerException, IOException {
        testCode("print \"/'?!<>[]{}()$&*=%;.|_-\\\"");
    }

    private void testCode(final String code) throws SessionManagerException, IOException {
        runner.enqueue(code);
        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteSparkInteractive.REL_WAIT);

        final String sessionId = "1";
        final Map<String, String> sessions = new LinkedHashMap<>();
        sessions.put("sessionId", sessionId);
        when(livySessionService.getSession()).thenReturn(sessions);

        when(livySessionService.getConnection()).thenReturn(httpClient);

        final HttpResponse jobId = getSuccessResponse();
        jobId.setEntity(new StringEntity("{\"id\":\"1\"}"));
        when(httpClient.execute(isA(HttpPost.class))).thenReturn(jobId);

        final HttpResponse jobState = getSuccessResponse();
        final String dataObject = "{\"completed\":1}";
        jobState.setEntity(new StringEntity(String.format("{\"state\":\"available\", \"output\":{\"data\":%s}}", dataObject)));
        when(httpClient.execute(isA(HttpGet.class))).thenReturn(jobState);

        runner.clearTransferState();
        runner.enqueue(code);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSparkInteractive.REL_SUCCESS);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExecuteSparkInteractive.REL_SUCCESS).iterator().next();
        flowFile.assertContentEquals(dataObject);
    }

    private HttpResponse getSuccessResponse() {
        return new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
    }
}
