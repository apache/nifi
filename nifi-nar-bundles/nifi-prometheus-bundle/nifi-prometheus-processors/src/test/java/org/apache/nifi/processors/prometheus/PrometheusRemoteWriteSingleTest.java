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
package org.apache.nifi.processors.prometheus;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.util.List;

public class PrometheusRemoteWriteSingleTest {

    private static final String REMOTE_WRITE_CONTEXT = "/test";
    private static final String REMOTE_WRITE_PORT = "0";

    private final String SINGLE_JSON_EXPECTED =
            "{\"metricLabels\":[" +
                    "{\"name\":\"name1\",\"value\":\"value1\"}," +
                    "{\"name\":\"name2\",\"value\":\"value2\"}]," +
            "\"metricSamples\":[" +
                    "{\"sample\":\"1.0\",\"timestamp\":\"1111111111111\"}]}";

    private TestRunner testRunner;
    private Thread spawnTestRunner;

    @Before
    public void init() throws InterruptedException {
        testRunner = TestRunners.newTestRunner(PrometheusRemoteWrite.class);
        testRunner.setProperty(PrometheusRemoteWrite.REMOTE_WRITE_CONTEXT, REMOTE_WRITE_CONTEXT);
        testRunner.setProperty(PrometheusRemoteWrite.REMOTE_WRITE_PORT, REMOTE_WRITE_PORT);

        TestRunnerInThread testRunnerInThread = new TestRunnerInThread(testRunner);
        spawnTestRunner = new Thread(testRunnerInThread);
        spawnTestRunner.start();
    }

    class TestRunnerInThread implements Runnable {
        private TestRunner testRunner;

        TestRunnerInThread(TestRunner testRunner) {
            this.testRunner = testRunner;
        }

        public void run() {
            testRunner.run();
        }
    }

    @Test
    public void testProcessorSingleMetric() throws Exception {
        // avoid race condition the test is quicker than Jetty startup.
        while (PrometheusRemoteWrite.serverEndpoint == null || !PrometheusRemoteWrite.serverEndpoint.isStarted()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        PrometheusMessage pm = new PrometheusMessage();
        byte[] compressedMessage = pm.getSingleMessage();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        ContentResponse response =
                httpClient.newRequest(PrometheusRemoteWrite.serverEndpoint.getURI().toString())
                        .method(HttpMethod.POST)
                        .content(new InputStreamContentProvider(
                                new ByteArrayInputStream(compressedMessage)))
                        .send();

        httpClient.stop();

        Assert.assertEquals(response.getStatus(), HttpServletResponse.SC_OK);
        testRunner.assertAllFlowFilesTransferred(PrometheusRemoteWrite.REL_SUCCESS);

        final List<MockFlowFile> flowFileList =
                testRunner.getFlowFilesForRelationship(PrometheusRemoteWrite.REL_SUCCESS);
        final MockFlowFile flowFile = flowFileList.get(0);
        final String content = flowFile.getContent();

        JsonObject expectedJson = JsonParser.parseString(SINGLE_JSON_EXPECTED).getAsJsonObject();
        JsonObject contentJson = JsonParser.parseString(content).getAsJsonObject();

        Assert.assertEquals(expectedJson, contentJson);
    }

    @After
    public void shutdown() throws Exception {
        if (PrometheusRemoteWrite.serverEndpoint != null){
            PrometheusRemoteWrite.serverEndpoint.stop();
        }
    }
}