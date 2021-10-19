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
package org.apache.nifi.processors.standard;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.nifi.processors.standard.InvokeHTTP.DELETE_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.GET_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.HEAD_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.OPTIONS_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.PATCH_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.POST_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.PUT_METHOD;

@RunWith(Parameterized.class)
public class InvokeHTTPParameterizedFileNameTest {

    private static final String BASE_PATH = "/";

    private static final String LOCALHOST = "localhost";

    private static final String FLOW_FILE_CONTENT = String.class.getName();

    private static final String FLOW_FILE_INITIAL_FILENAME = "the request FlowFile's filename";

    @SuppressWarnings("DefaultAnnotationParam")
    @Parameterized.Parameter(0)
    public String method;

    @Parameterized.Parameter(1)
    public String inputUrl;

    @Parameterized.Parameter(2)
    public String expectedFileName;

    private MockWebServer mockWebServer;

    private TestRunner runner;

    @Parameterized.Parameters(name = "When {0} http://baseUrl/{1}, filename of the response FlowFile should be {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {GET_METHOD, "", "localhost"},
                {GET_METHOD, "file", "file"},
                {GET_METHOD, "file/", "file"},
                {GET_METHOD, "file.txt", "file.txt"},
                {GET_METHOD, "file.txt/", "file.txt"},
                {GET_METHOD, "file.txt/?qp=v", "file.txt"},
                {POST_METHOD, "", FLOW_FILE_INITIAL_FILENAME},
                {PUT_METHOD, "", FLOW_FILE_INITIAL_FILENAME},
                {PATCH_METHOD, "", FLOW_FILE_INITIAL_FILENAME},
                {DELETE_METHOD, "", FLOW_FILE_INITIAL_FILENAME},
                {HEAD_METHOD, "", FLOW_FILE_INITIAL_FILENAME},
                {OPTIONS_METHOD, "", FLOW_FILE_INITIAL_FILENAME}
        });
    }

    @Before
    public void setRunner() {
        mockWebServer = new MockWebServer();
        runner = TestRunners.newTestRunner(new InvokeHTTP());
        // Disable Connection Pooling
        runner.setProperty(InvokeHTTP.PROP_MAX_IDLE_CONNECTIONS, Integer.toString(0));
    }

    @After
    public void shutdownServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testResponseFlowFileFilenameExtractedFromRemoteUrl() throws MalformedURLException {
        URL baseUrl = new URL(getMockWebServerUrl());
        URL targetUrl = new URL(baseUrl, inputUrl);

        runner.setProperty(InvokeHTTP.PROP_METHOD, method);
        runner.setProperty(InvokeHTTP.PROP_URL, targetUrl.toString());
        runner.setProperty(InvokeHTTP.UPDATE_FILENAME, Boolean.TRUE.toString());

        Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put(CoreAttributes.FILENAME.key(), FLOW_FILE_INITIAL_FILENAME);
        runner.enqueue(FLOW_FILE_CONTENT, ffAttributes);

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));

        runner.run();

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).iterator().next();
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), expectedFileName);
    }

    private String getMockWebServerUrl() {
        return mockWebServer.url(BASE_PATH).newBuilder().host(LOCALHOST).build().toString();
    }
}
