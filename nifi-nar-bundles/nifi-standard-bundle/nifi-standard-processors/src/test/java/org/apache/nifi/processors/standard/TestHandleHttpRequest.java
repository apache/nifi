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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

public class TestHandleHttpRequest {

    @Test(timeout=10000)
    public void testRequestAddedToService() throws InitializationException, MalformedURLException, IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpRequest.class);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        // trigger processor to stop but not shutdown.
        runner.run(1, false);
        try {
            final Thread httpThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                        final HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:"
                                + port + "/my/path?query=true&value1=value1&value2=&value3&value4=apple=orange").openConnection();
                        connection.setDoOutput(false);
                        connection.setRequestMethod("GET");
                        connection.setRequestProperty("header1", "value1");
                        connection.setRequestProperty("header2", "");
                        connection.setRequestProperty("header3", "apple=orange");
                        connection.setConnectTimeout(3000);
                        connection.setReadTimeout(3000);

                        StreamUtils.copy(connection.getInputStream(), new NullOutputStream());
                    } catch (final Throwable t) {
                        t.printStackTrace();
                        Assert.fail(t.toString());
                    }
                }
            });
            httpThread.start();

            while ( runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS).isEmpty() ) {
                // process the request.
                runner.run(1, false, false);
            }

            runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 1);
            assertEquals(1, contextMap.size());

            final MockFlowFile mff = runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS).get(0);
            mff.assertAttributeEquals("http.query.param.query", "true");
            mff.assertAttributeEquals("http.query.param.value1", "value1");
            mff.assertAttributeEquals("http.query.param.value2", "");
            mff.assertAttributeEquals("http.query.param.value3", "");
            mff.assertAttributeEquals("http.query.param.value4", "apple=orange");
            mff.assertAttributeEquals("http.headers.header1", "value1");
            mff.assertAttributeEquals("http.headers.header3", "apple=orange");
        } finally {
            // shut down the server
            runner.run(1, true);
        }
    }

    private static class MockHttpContextMap extends AbstractControllerService implements HttpContextMap {

        private final ConcurrentMap<String, HttpServletResponse> responseMap = new ConcurrentHashMap<>();

        @Override
        public boolean register(final String identifier, final HttpServletRequest request, final HttpServletResponse response, final AsyncContext context) {
            responseMap.put(identifier, response);
            return true;
        }

        @Override
        public HttpServletResponse getResponse(final String identifier) {
            return responseMap.get(identifier);
        }

        @Override
        public void complete(final String identifier) {
            responseMap.remove(identifier);
        }

        public int size() {
            return responseMap.size();
        }
    }
}
