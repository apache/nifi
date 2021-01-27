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
import static org.junit.Assert.assertTrue;

import java.net.URLEncoder;
import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.JettyServerUtils;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Integration Test for deprecated GetHTTP Processor
 */
@SuppressWarnings("deprecation")
public class ITGetHTTP {
    private static final String SSL_CONTEXT_IDENTIFIER = SSLContextService.class.getName();

    private static final String HTTP_URL = "http://localhost:%d";

    private static final String HTTPS_URL = "https://localhost:%d";

    private static SSLContext keyStoreSslContext;

    private static SSLContext trustStoreSslContext;

    private TestRunner controller;

    @BeforeClass
    public static void configureServices() throws TlsException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.GetHTTP", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestGetHTTP", "debug");

        keyStoreSslContext = SslContextUtils.createKeyStoreSslContext();
        trustStoreSslContext = SslContextUtils.createTrustStoreSslContext();
    }

    @Test
    public final void testContentModified() throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(RESTServiceContentModified.class, "/*");

        // create the service
        final int port = NetworkUtils.availablePort();
        final Server server = JettyServerUtils.createServer(port, null, null);
        server.setHandler(handler);

        try {
            JettyServerUtils.startServer(server);

            // this is the base url with the random port
            String destination = String.format(HTTP_URL, port);

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(org.apache.nifi.processors.standard.GetHTTP.class);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.FILENAME, "testFile");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");

            controller.getStateManager().assertStateNotSet(org.apache.nifi.processors.standard.GetHTTP.ETAG, Scope.LOCAL);
            controller.getStateManager().assertStateNotSet(org.apache.nifi.processors.standard.GetHTTP.LAST_MODIFIED, Scope.LOCAL);
            controller.run(2);

            // verify the lastModified and entityTag are updated
            controller.getStateManager().assertStateNotEquals(org.apache.nifi.processors.standard.GetHTTP.ETAG+":"+destination, "", Scope.LOCAL);
            controller.getStateManager().assertStateNotEquals(org.apache.nifi.processors.standard.GetHTTP.LAST_MODIFIED+":"+destination, "Thu, 01 Jan 1970 00:00:00 GMT", Scope.LOCAL);

            // ran twice, but got one...which is good
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);

            // verify remote.source flowfile attribute
            controller.getFlowFilesForRelationship(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS).get(0).assertAttributeEquals("gethttp.remote.source", "localhost");
            controller.clearTransferState();

            // turn off checking for etag and lastModified
            RESTServiceContentModified.IGNORE_ETAG = true;
            RESTServiceContentModified.IGNORE_LAST_MODIFIED = true;
            controller.run(2);

            // ran twice, got two...which is good
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 2);
            controller.clearTransferState();

            // turn on checking for etag
            RESTServiceContentModified.IGNORE_ETAG = false;
            controller.run(2);
            // ran twice, got 0...which is good
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 0);

            // turn on checking for lastModified, but off for etag
            RESTServiceContentModified.IGNORE_LAST_MODIFIED = false;
            RESTServiceContentModified.IGNORE_ETAG = true;
            controller.run(2);
            // ran twice, got 0...which is good
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 0);

            // turn off checking for lastModified, turn on checking for etag, but change the value
            RESTServiceContentModified.IGNORE_LAST_MODIFIED = true;
            RESTServiceContentModified.IGNORE_ETAG = false;
            RESTServiceContentModified.ETAG = 1;
            controller.run(2);

            // ran twice, got 1...but should have new cached etag
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);
            String eTagStateValue = controller.getStateManager().getState(Scope.LOCAL).get(org.apache.nifi.processors.standard.GetHTTP.ETAG+":"+destination);
            assertEquals("1",org.apache.nifi.processors.standard.GetHTTP.parseStateValue(eTagStateValue).getValue());
            controller.clearTransferState();

            // turn off checking for Etag, turn on checking for lastModified, but change value
            RESTServiceContentModified.IGNORE_LAST_MODIFIED = false;
            RESTServiceContentModified.IGNORE_ETAG = true;
            RESTServiceContentModified.modificationDate = System.currentTimeMillis() / 1000 * 1000 + 5000;
            String lastMod = controller.getStateManager().getState(Scope.LOCAL).get(org.apache.nifi.processors.standard.GetHTTP.LAST_MODIFIED+":"+destination);
            controller.run(2);

            // ran twice, got 1...but should have new cached etag
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);
            controller.getStateManager().assertStateNotEquals(org.apache.nifi.processors.standard.GetHTTP.LAST_MODIFIED+":"+destination, lastMod, Scope.LOCAL);
            controller.clearTransferState();

        } finally {
            // shutdown web service
            server.stop();
            server.destroy();
        }
    }

    @Test
    public final void testContentModifiedTwoServers() throws Exception {
        final int port1 = NetworkUtils.availablePort();
        final Server server1 = JettyServerUtils.createServer(port1, null, null);
        final ServletHandler handler1 = new ServletHandler();
        handler1.addServletWithMapping(RESTServiceContentModified.class, "/*");
        JettyServerUtils.addHandler(server1, handler1);

        final int port2 = NetworkUtils.availablePort();
        final Server server2 = JettyServerUtils.createServer(port2, null, null);
        final ServletHandler handler2 = new ServletHandler();
        handler2.addServletWithMapping(RESTServiceContentModified.class, "/*");
        JettyServerUtils.addHandler(server2, handler2);

        try {
            JettyServerUtils.startServer(server1);
            JettyServerUtils.startServer(server2);

            // this is the base urls with the random ports
            String destination1 = String.format(HTTP_URL, port1);
            String destination2 = String.format(HTTP_URL, port2);

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(org.apache.nifi.processors.standard.GetHTTP.class);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination1);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.FILENAME, "testFile");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");

            controller.getStateManager().assertStateNotSet(org.apache.nifi.processors.standard.GetHTTP.ETAG+":"+destination1, Scope.LOCAL);
            controller.getStateManager().assertStateNotSet(org.apache.nifi.processors.standard.GetHTTP.LAST_MODIFIED+":"+destination1, Scope.LOCAL);
            controller.run(2);

            // verify the lastModified and entityTag are updated
            controller.getStateManager().assertStateNotEquals(org.apache.nifi.processors.standard.GetHTTP.ETAG+":"+destination1, "", Scope.LOCAL);
            controller.getStateManager().assertStateNotEquals(org.apache.nifi.processors.standard.GetHTTP.LAST_MODIFIED+":"+destination1, "Thu, 01 Jan 1970 00:00:00 GMT", Scope.LOCAL);

            // ran twice, but got one...which is good
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);

            controller.clearTransferState();

            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination2);
            controller.getStateManager().assertStateNotSet(org.apache.nifi.processors.standard.GetHTTP.ETAG+":"+destination2, Scope.LOCAL);
            controller.getStateManager().assertStateNotSet(org.apache.nifi.processors.standard.GetHTTP.LAST_MODIFIED+":"+destination2, Scope.LOCAL);

            controller.run(2);

            // ran twice, but got one...which is good
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);

            // verify the lastModified's and entityTags are updated
            controller.getStateManager().assertStateNotEquals(org.apache.nifi.processors.standard.GetHTTP.ETAG+":"+destination1, "", Scope.LOCAL);
            controller.getStateManager().assertStateNotEquals(org.apache.nifi.processors.standard.GetHTTP.LAST_MODIFIED+":"+destination1, "Thu, 01 Jan 1970 00:00:00 GMT", Scope.LOCAL);
            controller.getStateManager().assertStateNotEquals(org.apache.nifi.processors.standard.GetHTTP.ETAG+":"+destination2, "", Scope.LOCAL);
            controller.getStateManager().assertStateNotEquals(org.apache.nifi.processors.standard.GetHTTP.LAST_MODIFIED+":"+destination2, "Thu, 01 Jan 1970 00:00:00 GMT", Scope.LOCAL);

        } finally {
            // shutdown web services
            server1.stop();
            server1.destroy();
            server2.stop();
            server2.destroy();
        }
    }

    @Test
    public final void testUserAgent() throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(UserAgentTestingServlet.class, "/*");

        // create the service
        final int port = NetworkUtils.availablePort();
        Server server = JettyServerUtils.createServer(port, null, null);
        server.setHandler(handler);

        try {
            JettyServerUtils.startServer(server);
            final String destination = String.format(HTTP_URL, port);

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(org.apache.nifi.processors.standard.GetHTTP.class);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.FILENAME, "testFile");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");

            controller.run();
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 0);

            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.USER_AGENT, "testUserAgent");
            controller.run();
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);

            // shutdown web service
        } finally {
            server.stop();
            server.destroy();
        }
    }

    @Test
    public final void testDynamicHeaders() throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(UserAgentTestingServlet.class, "/*");

        // create the service
        final int port = NetworkUtils.availablePort();
        Server server = JettyServerUtils.createServer(port, null, null);
        server.setHandler(handler);

        try {
            JettyServerUtils.startServer(server);
            final String destination = String.format(HTTP_URL, port);

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(org.apache.nifi.processors.standard.GetHTTP.class);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.FILENAME, "testFile");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.USER_AGENT, "testUserAgent");
            controller.setProperty("Static-Header", "StaticHeaderValue");
            controller.setProperty("EL-Header", "${now()}");

            controller.run();
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);

            // shutdown web service
        } finally {
            server.stop();
            server.destroy();
        }
    }

    @Test
    public final void testExpressionLanguage() throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(UserAgentTestingServlet.class, "/*");

        // create the service
        final int port = NetworkUtils.availablePort();
        Server server = JettyServerUtils.createServer(port, null, null);
        server.setHandler(handler);

        try {
            JettyServerUtils.startServer(server);
            final String destination = String.format(HTTP_URL, port);

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(org.apache.nifi.processors.standard.GetHTTP.class);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination+"/test_${literal(1)}.pdf");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.FILENAME, "test_${now():format('yyyy/MM/dd_HH:mm:ss')}");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.USER_AGENT, "testUserAgent");

            controller.run();
            controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);

            MockFlowFile response = controller.getFlowFilesForRelationship(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS).get(0);
            response.assertAttributeEquals("gethttp.remote.source","localhost");
            String fileName = response.getAttribute(CoreAttributes.FILENAME.key());
            assertTrue(fileName.matches("test_\\d\\d\\d\\d/\\d\\d/\\d\\d_\\d\\d:\\d\\d:\\d\\d"));
            // shutdown web service
        } finally {
            server.stop();
            server.destroy();
        }
    }

    /**
     * Test for HTTP errors
     * @throws Exception exception
     */
    @Test
    public final void testHttpErrors() throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HttpErrorServlet.class, "/*");

        // create the service
        final int port = NetworkUtils.availablePort();
        Server server = JettyServerUtils.createServer(port, null, null);
        server.setHandler(handler);

        try {
            JettyServerUtils.startServer(server);
            final String destination = String.format(HTTP_URL, port);
            HttpErrorServlet servlet = (HttpErrorServlet) handler.getServlets()[0].getServlet();

            this.controller = TestRunners.newTestRunner(org.apache.nifi.processors.standard.GetHTTP.class);
            this.controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            this.controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination+"/test_${literal(1)}.pdf");
            this.controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.FILENAME, "test_${now():format('yyyy/MM/dd_HH:mm:ss')}");
            this.controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");
            this.controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.USER_AGENT, "testUserAgent");

            // 204 - NO CONTENT
            servlet.setErrorToReturn(HttpServletResponse.SC_NO_CONTENT);
            this.controller.run();
            this.controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 0);

            // 404 - NOT FOUND
            servlet.setErrorToReturn(HttpServletResponse.SC_NOT_FOUND);
            this.controller.run();
            this.controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 0);

            // 500 - INTERNAL SERVER ERROR
            servlet.setErrorToReturn(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            this.controller.run();
            this.controller.assertTransferCount(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 0);
        } finally {
            // shutdown web service
            server.stop();
            server.destroy();
        }
    }

    @Test
    public final void testTlsClientAuthenticationNone() throws Exception {
        // set up web service
        final  ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HelloWorldServlet.class, "/*");

        // create the service, disabling the need for client auth
        final int port = NetworkUtils.availablePort();
        final Server server = JettyServerUtils.createServer(port, keyStoreSslContext, ClientAuth.NONE);
        server.setHandler(handler);

        try {
            JettyServerUtils.startServer(server);
            final String destination = String.format(HTTPS_URL, port);

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(org.apache.nifi.processors.standard.GetHTTP.class);
            // Use context service with only a truststore
            enableSslContextService(trustStoreSslContext);

            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.FILENAME, "testFile");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");

            controller.run();
            controller.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);
            final MockFlowFile mff = controller.getFlowFilesForRelationship(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS).get(0);
            mff.assertContentEquals("Hello, World!");
        } finally {
            server.stop();
            server.destroy();
        }
    }

    @Test
    public final void testTlsClientAuthenticationRequired() throws Exception {
        // set up web service
        final ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HelloWorldServlet.class, "/*");

        // create the service, providing both truststore and keystore properties, requiring client auth (default)
        final int port = NetworkUtils.availablePort();
        final Server server = JettyServerUtils.createServer(port, keyStoreSslContext, ClientAuth.REQUIRED);
        server.setHandler(handler);

        try {
            JettyServerUtils.startServer(server);
            final String destination = String.format(HTTPS_URL, port);

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(org.apache.nifi.processors.standard.GetHTTP.class);
            // Use context service with a keystore and a truststore
            enableSslContextService(keyStoreSslContext);

            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.CONNECTION_TIMEOUT, "10 secs");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.FILENAME, "testFile");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");

            controller.run();
            controller.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);
            final MockFlowFile mff = controller.getFlowFilesForRelationship(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS).get(0);
            mff.assertContentEquals("Hello, World!");
        } finally {
            server.stop();
            server.destroy();
        }
    }

    @Test
    public final void testCookiePolicy() throws Exception {
        final int port1 = NetworkUtils.availablePort();
        final Server server1 = JettyServerUtils.createServer(port1, null, null);
        final ServletHandler handler1 = new ServletHandler();
        handler1.addServletWithMapping(CookieTestingServlet.class, "/*");
        JettyServerUtils.addHandler(server1, handler1);

        final int port2 = NetworkUtils.availablePort();
        final Server server2 = JettyServerUtils.createServer(port2, null, null);
        final ServletHandler handler2 = new ServletHandler();
        handler2.addServletWithMapping(CookieVerificationTestingServlet.class, "/*");
        JettyServerUtils.addHandler(server2, handler2);

        try {
            JettyServerUtils.startServer(server1);
            JettyServerUtils.startServer(server2);

            // this is the base urls with the random ports
            String destination1 = String.format(HTTP_URL, port1);
            String destination2 = String.format(HTTP_URL, port2);

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(org.apache.nifi.processors.standard.GetHTTP.class);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination1 + "/?redirect=" + URLEncoder.encode(destination2, "UTF-8")
                    + "&datemode=" + CookieTestingServlet.DATEMODE_COOKIE_DEFAULT);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.FILENAME, "testFile");
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.FOLLOW_REDIRECTS, "true");

            controller.run(1);

            // verify default cookie data does successful redirect
            controller.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);
            MockFlowFile ff = controller.getFlowFilesForRelationship(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS).get(0);
            ff.assertContentEquals("Hello, World!");

            controller.clearTransferState();

            // verify NON-standard cookie data fails with default redirect_cookie_policy
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination1 + "/?redirect=" + URLEncoder.encode(destination2, "UTF-8")
                    + "&datemode=" + CookieTestingServlet.DATEMODE_COOKIE_NOT_TYPICAL);

            controller.run(1);

            controller.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 0);

            controller.clearTransferState();

            // change GetHTTP to place it in STANDARD cookie policy mode
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.REDIRECT_COOKIE_POLICY, org.apache.nifi.processors.standard.GetHTTP.STANDARD_COOKIE_POLICY_STR);
            controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.URL, destination1 + "/?redirect=" + URLEncoder.encode(destination2, "UTF-8")
                    + "&datemode=" + CookieTestingServlet.DATEMODE_COOKIE_NOT_TYPICAL);

            controller.run(1);

            // verify NON-standard cookie data does successful redirect
            controller.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS, 1);
            ff = controller.getFlowFilesForRelationship(org.apache.nifi.processors.standard.GetHTTP.REL_SUCCESS).get(0);
            ff.assertContentEquals("Hello, World!");

        } finally {
            // shutdown web services
            server1.stop();
            server1.destroy();
            server2.stop();
            server2.destroy();
        }
    }

    private void enableSslContextService(final SSLContext configuredSslContext) throws InitializationException {
        final SSLContextService sslContextService = Mockito.mock(SSLContextService.class);
        Mockito.when(sslContextService.getIdentifier()).thenReturn(SSL_CONTEXT_IDENTIFIER);
        Mockito.when(sslContextService.createContext()).thenReturn(configuredSslContext);
        controller.addControllerService(SSL_CONTEXT_IDENTIFIER, sslContextService);
        controller.enableControllerService(sslContextService);
        controller.setProperty(org.apache.nifi.processors.standard.GetHTTP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_IDENTIFIER);
    }
}
