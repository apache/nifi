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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

public class TestHandleHttpRequest {

    private static Map<String, String> getTruststoreProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        props.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        props.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        return props;
    }

    private static Map<String, String> getKeystoreProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        return properties;
    }

    private static SSLContext useSSLContextService(final TestRunner controller, final Map<String, String> sslProperties) {
        final SSLContextService service = new StandardRestrictedSSLContextService();
        try {
            controller.addControllerService("ssl-service", service, sslProperties);
            controller.enableControllerService(service);
        } catch (InitializationException ex) {
            ex.printStackTrace();
            Assert.fail("Could not create SSL Context Service");
        }

        controller.setProperty(HandleHttpRequest.SSL_CONTEXT, "ssl-service");
        return service.createSSLContext(SSLContextService.ClientAuth.WANT);
    }

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


    @Test(timeout=10000)
    public void testFailToRegister() throws InitializationException, MalformedURLException, IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpRequest.class);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");
        contextMap.setRegisterSuccessfully(false);

        // trigger processor to stop but not shutdown.
        runner.run(1, false);
        try {
            final int[] responseCode = new int[1];
            responseCode[0] = 0;
            final Thread httpThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    HttpURLConnection connection = null;
                    try {
                        final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                        connection = (HttpURLConnection) new URL("http://localhost:"
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
                        if(connection != null ) {
                            try {
                                responseCode[0] = connection.getResponseCode();
                            } catch (IOException e) {
                                responseCode[0] = -1;
                            }
                        } else {
                            responseCode[0] = -2;
                        }
                    }
                }
            });
            httpThread.start();

            while (responseCode[0] == 0) {
                // process the request.
                runner.run(1, false, false);
            }

            runner.assertTransferCount(HandleHttpRequest.REL_SUCCESS, 0);
            assertEquals(503, responseCode[0]);

        } finally {
            // shut down the server
            runner.run(1, true);
        }
    }

    @Test
    public void testSecure() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpRequest.class);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        final Map<String, String> sslProperties = getKeystoreProperties();
        sslProperties.putAll(getTruststoreProperties());
        sslProperties.put(StandardSSLContextService.SSL_ALGORITHM.getName(), "TLSv1.2");
        final SSLContext sslContext = useSSLContextService(runner, sslProperties);

        // trigger processor to stop but not shutdown.
        runner.run(1, false);
        try {
            final Thread httpThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                        final HttpsURLConnection connection = (HttpsURLConnection) new URL("https://localhost:"
                                + port + "/my/path?query=true&value1=value1&value2=&value3&value4=apple=orange").openConnection();

                        connection.setSSLSocketFactory(sslContext.getSocketFactory());
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
            mff.assertAttributeEquals("http.protocol", "HTTP/1.1");
        } finally {
            // shut down the server
            runner.run(1, true);
        }
    }

    private static class MockHttpContextMap extends AbstractControllerService implements HttpContextMap {

        private boolean registerSuccessfully = true;

        private final ConcurrentMap<String, HttpServletResponse> responseMap = new ConcurrentHashMap<>();

        @Override
        public boolean register(final String identifier, final HttpServletRequest request, final HttpServletResponse response, final AsyncContext context) {
            if(registerSuccessfully) {
                responseMap.put(identifier, response);
            }
            return registerSuccessfully;
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

        public boolean isRegisterSuccessfully() {
            return registerSuccessfully;
        }

        public void setRegisterSuccessfully(boolean registerSuccessfully) {
            this.registerSuccessfully = registerSuccessfully;
        }
    }
}
