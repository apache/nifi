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

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

public class SSLContextServiceTest {

    @Test
    public void testBad1() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<String, String>();
        runner.addControllerService("test-bad1", service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testBad2() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<String, String>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        runner.addControllerService("test-bad2", service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testBad3() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<String, String>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        runner.addControllerService("test-bad3", service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testBad4() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<String, String>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "wrongpassword");
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "PKCS12");
        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "wrongpassword");
        properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        runner.addControllerService("test-bad4", service, properties);
        
        runner.assertNotValid(service);
    }

    @Test
    public void testBad5() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<String, String>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/DOES-NOT-EXIST.jks");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "PKCS12");
        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        runner.addControllerService("test-bad5", service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testGood() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        SSLContextService service = new StandardSSLContextService();
        runner.addControllerService("test-good1", service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        runner.enableControllerService(service);

        runner.setProperty("SSL Context Svc ID", "test-good1");
        runner.assertValid(service);
        service = (SSLContextService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-good1");
        Assert.assertNotNull(service);
        SSLContextService sslService = (SSLContextService) service;
        sslService.createSSLContext(ClientAuth.REQUIRED);
        sslService.createSSLContext(ClientAuth.WANT);
        sslService.createSSLContext(ClientAuth.NONE);
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
            runner.enableControllerService(service);
            
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
            runner.enableControllerService(service);

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
