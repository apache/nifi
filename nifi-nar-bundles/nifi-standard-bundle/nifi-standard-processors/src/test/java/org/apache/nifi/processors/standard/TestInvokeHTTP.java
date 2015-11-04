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

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.processors.standard.util.TestInvokeHttpCommon;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestInvokeHTTP extends TestInvokeHttpCommon {

    @BeforeClass
    public static void beforeClass() throws Exception {
        // useful for verbose logging output
        // don't commit this with this property enabled, or any 'mvn test' will be really verbose
        // System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard", "debug");

        // create a Jetty server on a random port
        server = createServer();
        server.startServer();

        // this is the base url with the random port
        url = server.getUrl();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        server.shutdownServer();
    }

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(InvokeHTTP.class);

        server.clearHandlers();
    }

    @After
    public void after() {
        runner.shutdown();
    }

    private static TestServer createServer() throws IOException {
        return new TestServer();
    }

    // Currently InvokeHttp does not support Proxy via Https
    @Test
    public void testProxy() throws Exception {
        addHandler(new MyProxyHandler());
        URL proxyURL = new URL(url);

        runner.setProperty(InvokeHTTP.Config.PROP_URL, "http://nifi.apache.org/"); // just a dummy URL no connection goes out
        runner.setProperty(InvokeHTTP.Config.PROP_PROXY_HOST, proxyURL.getHost());
        runner.setProperty(InvokeHTTP.Config.PROP_PROXY_PORT, String.valueOf(proxyURL.getPort()));

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.Config.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.Config.REL_SUCCESS_RESP, 1);
        runner.assertTransferCount(InvokeHTTP.Config.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.Config.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.Config.REL_FAILURE, 0);

        //expected in request status.code and status.message
        //original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.Config.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.Config.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.Config.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        //expected in response
        //status code, status message, all headers from server response --> ff attributes
        //server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.Config.REL_SUCCESS_RESP).get(0);
        bundle1.assertContentEquals("http://nifi.apache.org/".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.Config.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.Config.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain; charset=ISO-8859-1");
    }

    public static class MyProxyHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            if ("Get".equalsIgnoreCase(request.getMethod())) {
                response.setStatus(200);
                String proxyPath = baseRequest.getUri().toString();
                response.setContentLength(proxyPath.length());
                response.setContentType("text/plain");

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(proxyPath);
                    writer.flush();
                }
            } else {
                response.setStatus(404);
                response.setContentType("text/plain");
                response.setContentLength(0);
            }
        }
    }
}
