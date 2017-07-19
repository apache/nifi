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
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

/**
 * Unit tests for the ControlNiFiComponent processor.
 */
public class TestControlNiFiComponent {

    @Test
    public void testValidUnsecured() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "http://localhost:8080/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "1 sec");
        runner.setProperty(ControlNiFiComponent.UUID, "${processor}");

        runner.assertValid();
    }

    @Test
    public void testNotValidSecured() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "https://localhost:8443/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "1 sec");
        runner.setProperty(ControlNiFiComponent.UUID, "${processor}");

        runner.assertNotValid();
    }

    @Test
    public void testNotValidSecuredNoLoginPassword() throws IOException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        final Map<String, String> sslProperties = new HashMap<>();
        sslProperties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");

        final StandardSSLContextService sslService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslService, sslProperties);
        runner.enableControllerService(sslService);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "https://localhost:8443/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "1 sec");
        runner.setProperty(ControlNiFiComponent.UUID, "${processor}");
        runner.setProperty(ControlNiFiComponent.SSL_CONTEXT_SERVICE, "ssl-context");

        runner.assertNotValid();
    }

    @Test
    public void testValidSecuredLoginPassword() throws IOException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        final Map<String, String> sslProperties = new HashMap<>();
        sslProperties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");

        final StandardSSLContextService sslService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslService, sslProperties);
        runner.enableControllerService(sslService);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "https://localhost:8443/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "1 sec");
        runner.setProperty(ControlNiFiComponent.UUID, "${processor}");
        runner.setProperty(ControlNiFiComponent.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.setProperty(ControlNiFiComponent.USERNAME, "admin");
        runner.setProperty(ControlNiFiComponent.PASSWORD, "password");

        runner.assertValid();
    }

    @Test
    public void testValidSecuredCerts() throws IOException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        final Map<String, String> sslProperties = new HashMap<>();
        sslProperties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        sslProperties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        sslProperties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        sslProperties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");

        final StandardSSLContextService sslService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslService, sslProperties);
        runner.enableControllerService(sslService);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "https://localhost:8443/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "1 sec");
        runner.setProperty(ControlNiFiComponent.UUID, "${processor}");
        runner.setProperty(ControlNiFiComponent.SSL_CONTEXT_SERVICE, "ssl-context");

        runner.assertValid();
    }

}