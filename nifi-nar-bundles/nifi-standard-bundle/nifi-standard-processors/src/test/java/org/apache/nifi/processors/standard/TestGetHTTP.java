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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestGetHTTP {

    private TestRunner controller;

    @BeforeClass
    public static void before() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.GetHTTP", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestGetHTTP", "debug");
    }

    @Test
    public final void testContentModified() throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(RESTServiceContentModified.class, "/*");

        // create the service
        TestServer server = new TestServer();
        server.addHandler(handler);

        try {
            server.startServer();

            // this is the base url with the random port
            String destination = server.getUrl();

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(GetHTTP.class);
            controller.setProperty(GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(GetHTTP.URL, destination);
            controller.setProperty(GetHTTP.FILENAME, "testFile");
            controller.setProperty(GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");

            controller.getStateManager().assertStateNotSet(GetHTTP.ETAG, Scope.LOCAL);
            controller.getStateManager().assertStateNotSet(GetHTTP.LAST_MODIFIED, Scope.LOCAL);
            controller.run(2);

            // verify the lastModified and entityTag are updated
            controller.getStateManager().assertStateNotEquals(GetHTTP.ETAG, "", Scope.LOCAL);
            controller.getStateManager().assertStateNotEquals(GetHTTP.LAST_MODIFIED, "Thu, 01 Jan 1970 00:00:00 GMT", Scope.LOCAL);

            // ran twice, but got one...which is good
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 1);

            // verify remote.source flowfile attribute
            controller.getFlowFilesForRelationship(GetHTTP.REL_SUCCESS).get(0).assertAttributeEquals("gethttp.remote.source", "localhost");

            controller.clearTransferState();

            // turn off checking for etag and lastModified
            RESTServiceContentModified.IGNORE_ETAG = true;
            RESTServiceContentModified.IGNORE_LAST_MODIFIED = true;
            controller.run(2);
            // ran twice, got two...which is good
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 2);
            controller.clearTransferState();

            // turn on checking for etag
            RESTServiceContentModified.IGNORE_ETAG = false;
            controller.run(2);
            // ran twice, got 0...which is good
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 0);

            // turn on checking for lastModified, but off for etag
            RESTServiceContentModified.IGNORE_LAST_MODIFIED = false;
            RESTServiceContentModified.IGNORE_ETAG = true;
            controller.run(2);
            // ran twice, got 0...which is good
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 0);

            // turn off checking for lastModified, turn on checking for etag, but change the value
            RESTServiceContentModified.IGNORE_LAST_MODIFIED = true;
            RESTServiceContentModified.IGNORE_ETAG = false;
            RESTServiceContentModified.ETAG = 1;
            controller.run(2);
            // ran twice, got 1...but should have new cached etag
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 1);
            controller.getStateManager().assertStateEquals(GetHTTP.ETAG, "1", Scope.LOCAL);
            controller.clearTransferState();

            // turn off checking for Etag, turn on checking for lastModified, but change value
            RESTServiceContentModified.IGNORE_LAST_MODIFIED = false;
            RESTServiceContentModified.IGNORE_ETAG = true;
            RESTServiceContentModified.modificationDate = System.currentTimeMillis() / 1000 * 1000 + 5000;
            String lastMod = controller.getStateManager().getState(Scope.LOCAL).get(GetHTTP.LAST_MODIFIED);
            controller.run(2);
            // ran twice, got 1...but should have new cached etag
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 1);
            controller.getStateManager().assertStateNotEquals(GetHTTP.LAST_MODIFIED, lastMod, Scope.LOCAL);
            controller.clearTransferState();

            // shutdown web service
        } finally {
            server.shutdownServer();
        }
    }


    @Test
    public final void testUserAgent() throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(UserAgentTestingServlet.class, "/*");

        // create the service
        TestServer server = new TestServer();
        server.addHandler(handler);

        try {
            server.startServer();

            String destination = server.getUrl();

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(GetHTTP.class);
            controller.setProperty(GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(GetHTTP.URL, destination);
            controller.setProperty(GetHTTP.FILENAME, "testFile");
            controller.setProperty(GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");

            controller.run();
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 0);

            controller.setProperty(GetHTTP.USER_AGENT, "testUserAgent");
            controller.run();
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 1);

            // shutdown web service
        } finally {
            server.shutdownServer();
        }
    }

    @Test
    public final void testExpressionLanguage() throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(UserAgentTestingServlet.class, "/*");

        // create the service
        TestServer server = new TestServer();
        server.addHandler(handler);

        try {
            server.startServer();

            String destination = server.getUrl();

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(GetHTTP.class);
            controller.setProperty(GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(GetHTTP.URL, destination+"/test_${literal(1)}.pdf");
            controller.setProperty(GetHTTP.FILENAME, "test_${now():format('yyyy/MM/dd_HH:mm:ss')}");
            controller.setProperty(GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");
            controller.setProperty(GetHTTP.USER_AGENT, "testUserAgent");

            controller.run();
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 1);

            MockFlowFile response = controller.getFlowFilesForRelationship(GetHTTP.REL_SUCCESS).get(0);
            response.assertAttributeEquals("gethttp.remote.source","localhost");
            String fileName = response.getAttribute(CoreAttributes.FILENAME.key());
            assertTrue(fileName.matches("test_\\d\\d\\d\\d/\\d\\d/\\d\\d_\\d\\d:\\d\\d:\\d\\d"));
            // shutdown web service
        } finally {
            server.shutdownServer();
        }
    }

    @Test
    public final void testSecure_oneWaySsl() throws Exception {
        // set up web service
        final  ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HelloWorldServlet.class, "/*");

        // create the service, disabling the need for client auth
        final Map<String, String> serverSslProperties = getKeystoreProperties();
        serverSslProperties.put(TestServer.NEED_CLIENT_AUTH, Boolean.toString(false));
        final TestServer server = new TestServer(serverSslProperties);
        server.addHandler(handler);

        try {
            server.startServer();

            final String destination = server.getSecureUrl();

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(GetHTTP.class);
            // Use context service with only a truststore
            useSSLContextService(getTruststoreProperties());

            controller.setProperty(GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(GetHTTP.URL, destination);
            controller.setProperty(GetHTTP.FILENAME, "testFile");
            controller.setProperty(GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");

            controller.run();
            controller.assertAllFlowFilesTransferred(GetHTTP.REL_SUCCESS, 1);
            final MockFlowFile mff = controller.getFlowFilesForRelationship(GetHTTP.REL_SUCCESS).get(0);
            mff.assertContentEquals("Hello, World!");
        } finally {
            server.shutdownServer();
        }
    }

    @Test
    public final void testSecure_twoWaySsl() throws Exception {
        // set up web service
        final ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HelloWorldServlet.class, "/*");

        // create the service, providing both truststore and keystore properties, requiring client auth (default)
        final Map<String, String> twoWaySslProperties = getKeystoreProperties();
        twoWaySslProperties.putAll(getTruststoreProperties());
        final TestServer server = new TestServer(twoWaySslProperties);
        server.addHandler(handler);

        try {
            server.startServer();

            final String destination = server.getSecureUrl();

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(GetHTTP.class);
            // Use context service with a keystore and a truststore
            useSSLContextService(twoWaySslProperties);

            controller.setProperty(GetHTTP.CONNECTION_TIMEOUT, "10 secs");
            controller.setProperty(GetHTTP.URL, destination);
            controller.setProperty(GetHTTP.FILENAME, "testFile");
            controller.setProperty(GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");

            controller.run();
            controller.assertAllFlowFilesTransferred(GetHTTP.REL_SUCCESS, 1);
            final MockFlowFile mff = controller.getFlowFilesForRelationship(GetHTTP.REL_SUCCESS).get(0);
            mff.assertContentEquals("Hello, World!");
        } finally {
            server.shutdownServer();
        }
    }

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

    private void useSSLContextService(final Map<String, String> sslProperties) {
        final SSLContextService service = new StandardSSLContextService();
        try {
            controller.addControllerService("ssl-service", service, sslProperties);
            controller.enableControllerService(service);
        } catch (InitializationException ex) {
            ex.printStackTrace();
            Assert.fail("Could not create SSL Context Service");
        }

        controller.setProperty(GetHTTP.SSL_CONTEXT_SERVICE, "ssl-service");
    }

}
