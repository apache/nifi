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

import com.google.gson.Gson;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.util.List;

public class PrometheusRemoteWriteBatchTest {

    private static final String REMOTE_WRITE_CONTEXT = "/test";
    private static final String REMOTE_WRITE_PORT = "0";
    private static final String MAX_BATCH_METRICS = "2";

    private final String BATCH_JSON_EXPECTED =
            "[{\"metricLabels\":[" +
                    "{\"name\":\"name1\",\"value\":\"value1\"}]," +
              "\"metricSamples\":[" +
                    "{\"sample\":\"1.0\",\"timestamp\":\"1111111111111\"}]}," +
              "{\"metricLabels\":[" +
                    "{\"name\":\"name2\",\"value\":\"value2\"}]," +
              "\"metricSamples\":[" +
                    "{\"sample\":\"2.0\",\"timestamp\":\"2222222222222\"}]}]";

    private TestRunner testRunner;
    private Thread spawnTestRunner;

    @Before
    public void init() throws InterruptedException {
        testRunner = TestRunners.newTestRunner(PrometheusRemoteWrite.class);
        testRunner.setProperty(PrometheusRemoteWrite.REMOTE_WRITE_CONTEXT, REMOTE_WRITE_CONTEXT);
        testRunner.setProperty(PrometheusRemoteWrite.REMOTE_WRITE_PORT, REMOTE_WRITE_PORT);
        testRunner.setProperty(PrometheusRemoteWrite.MAX_BATCH_METRICS, MAX_BATCH_METRICS);

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
    public void testProcessorBatchMetrics() throws Exception {
        // avoid race condition the test is quicker than Jetty startup.
        while (PrometheusRemoteWrite.serverEndpoint == null || !PrometheusRemoteWrite.serverEndpoint.isStarted()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        PrometheusMessage pm1 = new PrometheusMessage();
        byte[] compressedMessage1 = pm1.getBatchMessage("name1",
                "value1",
                1.0,
                1111111111111L);

        PrometheusMessage pm2 = new PrometheusMessage();
        byte[] compressedMessage2 = pm2.getBatchMessage("name2",
                "value2",
                2.0,
                2222222222222L);

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        ContentResponse response1 =
                httpClient.newRequest(PrometheusRemoteWrite.serverEndpoint.getURI().toString())
                        .method(HttpMethod.POST)
                        .content(new InputStreamContentProvider(
                                new ByteArrayInputStream(compressedMessage1)))
                        .send();

        Assert.assertEquals(response1.getStatus(), HttpServletResponse.SC_OK);

        ContentResponse response2 =
                httpClient.newRequest(PrometheusRemoteWrite.serverEndpoint.getURI().toString())
                        .method(HttpMethod.POST)
                        .content(new InputStreamContentProvider(
                                new ByteArrayInputStream(compressedMessage2)))
                        .send();

        httpClient.stop();

        Assert.assertEquals(response2.getStatus(), HttpServletResponse.SC_OK);

        testRunner.assertAllFlowFilesTransferred(PrometheusRemoteWrite.REL_SUCCESS);

        final List<MockFlowFile> flowFileList =
                testRunner.getFlowFilesForRelationship(PrometheusRemoteWrite.REL_SUCCESS);
        final MockFlowFile flowFile = flowFileList.get(0);
        final String content = flowFile.getContent();

        Gson gson1 = new Gson();
        String contentJson = gson1.toJson(content);

        Gson gson2 = new Gson();
        String expectedJson = gson2.toJson(BATCH_JSON_EXPECTED);

        Assert.assertEquals(expectedJson, contentJson);
    }

    @After
    public void shutdown() throws Exception {
        if (PrometheusRemoteWrite.serverEndpoint != null){
            PrometheusRemoteWrite.serverEndpoint.stop();
        }
    }
}