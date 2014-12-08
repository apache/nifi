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
package org.apache.nifi.ssl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Assert;
import org.junit.Test;

public class SSLContextServiceTest {

    @Test
    public void testBad1() {
        try {
            TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
            SSLContextService service = new StandardSSLContextService();
            HashMap<String, String> properties = new HashMap<String, String>();
            runner.addControllerService("test-bad1", service, properties);
            Assert.fail("Should have thrown an Exception");
        } catch (InitializationException e) {
            assertEquals(
                    "org.apache.nifi.reporting.InitializationException: SSLContextService[id=test-bad1] does not have the KeyStore or the TrustStore populated",
                    e.getCause().getCause().toString());
        }
    }

    @Test
    public void testBad2() {
        try {
            TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
            SSLContextService service = new StandardSSLContextService();
            HashMap<String, String> properties = new HashMap<String, String>();
            properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
            properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
            runner.addControllerService("test-bad2", service, properties);
            Assert.fail("Should have thrown an Exception");
        } catch (InitializationException e) {
            assertEquals(
                    "org.apache.nifi.reporting.InitializationException: SSLContextService[id=test-bad2] is not valid due to:\n'Keystore Properties' is invalid because Must set either 0 or 3 properties for Keystore",
                    e.getCause().getCause().toString());
        }
    }

    @Test
    public void testBad3() {
        try {
            TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
            SSLContextService service = new StandardSSLContextService();
            HashMap<String, String> properties = new HashMap<String, String>();
            properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
            properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
            properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
            properties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
            runner.addControllerService("test-bad3", service, properties);
            Assert.fail("Should have thrown an Exception");
        } catch (InitializationException e) {
            assertEquals(
                    "org.apache.nifi.reporting.InitializationException: SSLContextService[id=test-bad3] is not valid due to:\n'Truststore Properties' is invalid because Must set either 0 or 3 properties for Truststore",
                    e.getCause().getCause().toString());
        }
    }

    @Test
    public void testBad4() {
        try {
            TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
            SSLContextService service = new StandardSSLContextService();
            HashMap<String, String> properties = new HashMap<String, String>();
            properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
            properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "wrongpassword");
            properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "PKCS12");
            properties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
            properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "wrongpassword");
            properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
            runner.addControllerService("test-bad4", service, properties);
            Assert.fail("Should have thrown an Exception");
        } catch (InitializationException e) {
            assertEquals(
                    "org.apache.nifi.reporting.InitializationException: SSLContextService[id=test-bad4] is not valid due to:\n"
                    + "'Keystore Properties' is invalid because Invalid KeyStore Password or Type specified for file src/test/resources/localhost-ks.jks\n"
                    + "'Truststore Properties' is invalid because Invalid KeyStore Password or Type specified for file src/test/resources/localhost-ts.jks",
                    e.getCause().getCause().toString());
        }
    }

    @Test
    public void testBad5() {
        try {
            TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
            SSLContextService service = new StandardSSLContextService();
            HashMap<String, String> properties = new HashMap<String, String>();
            properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/DOES-NOT-EXIST.jks");
            properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
            properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "PKCS12");
            properties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
            properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
            properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
            runner.addControllerService("test-bad5", service, properties);
            Assert.fail("Should have thrown an Exception");
        } catch (InitializationException e) {
            assertTrue(e.getCause().getCause().toString().startsWith("org.apache.nifi.reporting.InitializationException: "
                    + "SSLContextService[id=test-bad5] is not valid due to:\n'Keystore Properties' is invalid "
                    + "because Cannot access file"));
        }
    }

    @Test
    public void testGood() {
        try {
            TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
            ControllerService service = new StandardSSLContextService();
            HashMap<String, String> properties = new HashMap<String, String>();
            properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
            properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
            properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "PKCS12");
            properties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
            properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
            properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
            runner.addControllerService("test-good1", service, properties);
            runner.setProperty("SSL Context Svc ID", "test-good1");
            runner.assertValid();
            service = runner.getProcessContext().getControllerServiceLookup().getControllerService("test-good1");
            Assert.assertNotNull(service);
            Assert.assertTrue(service instanceof StandardSSLContextService);
            SSLContextService sslService = (SSLContextService) service;
            sslService.createSSLContext(ClientAuth.REQUIRED);
            sslService.createSSLContext(ClientAuth.WANT);
            sslService.createSSLContext(ClientAuth.NONE);
        } catch (InitializationException e) {
        }
    }

    @Test
    public void testGoodTrustOnly() {
        try {
            TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
            SSLContextService service = new StandardSSLContextService();
            HashMap<String, String> properties = new HashMap<String, String>();
            properties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
            properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
            properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
            runner.addControllerService("test-good2", service, properties);
            runner.setProperty("SSL Context Svc ID", "test-good2");
            runner.assertValid();
            Assert.assertNotNull(service);
            Assert.assertTrue(service instanceof StandardSSLContextService);
            service.createSSLContext(ClientAuth.NONE);
        } catch (InitializationException e) {
        }
    }

    @Test
    public void testGoodKeyOnly() {
        try {
            TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
            SSLContextService service = new StandardSSLContextService();
            HashMap<String, String> properties = new HashMap<String, String>();
            properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
            properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
            properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
            runner.addControllerService("test-good3", service, properties);
            runner.setProperty("SSL Context Svc ID", "test-good3");
            runner.assertValid();
            Assert.assertNotNull(service);
            Assert.assertTrue(service instanceof StandardSSLContextService);
            SSLContextService sslService = service;
            sslService.createSSLContext(ClientAuth.NONE);
        } catch (Exception e) {
            System.out.println(e);
            Assert.fail("Should not have thrown a exception " + e.getMessage());
        }
    }

}
