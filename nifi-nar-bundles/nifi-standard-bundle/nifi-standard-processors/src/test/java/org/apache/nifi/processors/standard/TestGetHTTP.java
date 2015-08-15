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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
        File confDir = new File("conf");
        if (!confDir.exists()) {
            confDir.mkdir();
        }
    }

    @AfterClass
    public static void after() {
        File confDir = new File("conf");
        assertTrue(confDir.exists());
        File[] files = confDir.listFiles();
        if (files.length > 0) {
            for (File file : files) {
                assertTrue("Failed to delete " + file.getName(), file.delete());
            }
        }
        assertTrue(confDir.delete());
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

            GetHTTP getHTTPProcessor = (GetHTTP) controller.getProcessor();
            assertEquals("", getHTTPProcessor.entityTagRef.get());
            assertEquals("Thu, 01 Jan 1970 00:00:00 GMT", getHTTPProcessor.lastModifiedRef.get());
            controller.run(2);

            // verify the lastModified and entityTag are updated
            assertFalse("".equals(getHTTPProcessor.entityTagRef.get()));
            assertFalse("Thu, 01 Jan 1970 00:00:00 GMT".equals(getHTTPProcessor.lastModifiedRef.get()));
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
            assertEquals("1", getHTTPProcessor.entityTagRef.get());
            controller.clearTransferState();

            // turn off checking for Etag, turn on checking for lastModified, but change value
            RESTServiceContentModified.IGNORE_LAST_MODIFIED = false;
            RESTServiceContentModified.IGNORE_ETAG = true;
            RESTServiceContentModified.modificationDate = System.currentTimeMillis() / 1000 * 1000 + 5000;
            String lastMod = getHTTPProcessor.lastModifiedRef.get();
            controller.run(2);
            // ran twice, got 1...but should have new cached etag
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 1);
            assertFalse(lastMod.equals(getHTTPProcessor.lastModifiedRef.get()));
            controller.clearTransferState();

            // shutdown web service
        } finally {
            server.shutdownServer();
        }
    }

    @Test
    public void testPersistEtagLastMod() throws Exception {
        // delete the config file
        File confDir = new File("conf");
        File[] files = confDir.listFiles();
        for (File file : files) {
            assertTrue("Failed to delete " + file.getName(), file.delete());
        }

        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(RESTServiceContentModified.class, "/*");

        // create the service
        TestServer server = new TestServer();
        server.addHandler(handler);

        try {
            server.startServer();

            // get the server url
            String destination = server.getUrl();

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(GetHTTP.class);
            controller.setProperty(GetHTTP.CONNECTION_TIMEOUT, "5 secs");
            controller.setProperty(GetHTTP.FILENAME, "testFile");
            controller.setProperty(GetHTTP.URL, destination);
            controller.setProperty(GetHTTP.ACCEPT_CONTENT_TYPE, "application/json");

            GetHTTP getHTTPProcessor = (GetHTTP) controller.getProcessor();

            assertEquals("", getHTTPProcessor.entityTagRef.get());
            assertEquals("Thu, 01 Jan 1970 00:00:00 GMT", getHTTPProcessor.lastModifiedRef.get());
            controller.run(2);

            // verify the lastModified and entityTag are updated
            String etag = getHTTPProcessor.entityTagRef.get();
            assertFalse("".equals(etag));
            String lastMod = getHTTPProcessor.lastModifiedRef.get();
            assertFalse("Thu, 01 Jan 1970 00:00:00 GMT".equals(lastMod));
            // ran twice, but got one...which is good
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 1);
            controller.clearTransferState();

            files = confDir.listFiles();
            assertEquals(1, files.length);
            File file = files[0];
            assertTrue(file.exists());
            Properties props = new Properties();
            FileInputStream fis = new FileInputStream(file);
            props.load(fis);
            fis.close();
            assertEquals(etag, props.getProperty(GetHTTP.ETAG));
            assertEquals(lastMod, props.getProperty(GetHTTP.LAST_MODIFIED));

            ProcessorInitializationContext pic = new MockProcessorInitializationContext(controller.getProcessor(), (MockProcessContext) controller.getProcessContext());
            // init causes read from file
            getHTTPProcessor.init(pic);
            assertEquals(etag, getHTTPProcessor.entityTagRef.get());
            assertEquals(lastMod, getHTTPProcessor.lastModifiedRef.get());
            controller.run(2);
            // ran twice, got none...which is good
            controller.assertTransferCount(GetHTTP.REL_SUCCESS, 0);
            controller.clearTransferState();
            files = confDir.listFiles();
            assertEquals(1, files.length);
            file = files[0];
            assertTrue(file.exists());
            props = new Properties();
            fis = new FileInputStream(file);
            props.load(fis);
            fis.close();
            assertEquals(etag, props.getProperty(GetHTTP.ETAG));
            assertEquals(lastMod, props.getProperty(GetHTTP.LAST_MODIFIED));

            getHTTPProcessor.onRemoved();
            assertFalse(file.exists());

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

    private Map<String, String> getSslProperties() {
        Map<String, String> props = new HashMap<String, String>();
        props.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        props.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        props.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        props.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        props.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        props.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        return props;
    }

    private void useSSLContextService() {
        final SSLContextService service = new StandardSSLContextService();
        try {
            controller.addControllerService("ssl-service", service, getSslProperties());
            controller.enableControllerService(service);
        } catch (InitializationException ex) {
            ex.printStackTrace();
            Assert.fail("Could not create SSL Context Service");
        }

        controller.setProperty(GetHTTP.SSL_CONTEXT_SERVICE, "ssl-service");
    }

    @Test
    public final void testSecure() throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HelloWorldServlet.class, "/*");

        // create the service
        TestServer server = new TestServer(getSslProperties());
        server.addHandler(handler);

        try {
            server.startServer();

            String destination = server.getSecureUrl();

            // set up NiFi mock controller
            controller = TestRunners.newTestRunner(GetHTTP.class);
            useSSLContextService();

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

}
