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
package org.apache.nifi.websocket.jetty;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TestJettyWebSocketClient {

    public static final String CUSTOM_AUTH = "Apikey 8743b52063cd84097a65d1633f5c74f5";

    @Test
    public void testValidationRequiredProperties() throws Exception {
        final JettyWebSocketClient service = new JettyWebSocketClient();
        final ControllerServiceTestContext context = new ControllerServiceTestContext(service, "service-id");
        service.initialize(context.getInitializationContext());
        final Collection<ValidationResult> results = service.validate(context.getValidationContext());
        assertEquals(1, results.size());
        final ValidationResult result = results.iterator().next();
        assertEquals(JettyWebSocketClient.WS_URI.getDisplayName(), result.getSubject());
    }

    @Test
    public void testValidationSuccess() throws Exception {
        final JettyWebSocketClient service = new JettyWebSocketClient();
        final ControllerServiceTestContext context = new ControllerServiceTestContext(service, "service-id");
        context.setCustomValue(JettyWebSocketClient.WS_URI, "ws://localhost:9001/test");
        service.initialize(context.getInitializationContext());
        final Collection<ValidationResult> results = service.validate(context.getValidationContext());
        assertEquals(0, results.size());
    }

    @Test
    public void testValidationProtocol() throws Exception {
        final JettyWebSocketClient service = new JettyWebSocketClient();
        final ControllerServiceTestContext context = new ControllerServiceTestContext(service, "service-id");
        context.setCustomValue(JettyWebSocketClient.WS_URI, "http://localhost:9001/test");
        service.initialize(context.getInitializationContext());
        final Collection<ValidationResult> results = service.validate(context.getValidationContext());
        assertEquals(1, results.size());
        final ValidationResult result = results.iterator().next();
        assertEquals(JettyWebSocketClient.WS_URI.getName(), result.getSubject());
    }

    @Test
    public void testValidationProxyHostOnly() throws Exception {
        final JettyWebSocketClient service = new JettyWebSocketClient();
        final ControllerServiceTestContext context = new ControllerServiceTestContext(service, "service-id");
        context.setCustomValue(JettyWebSocketClient.WS_URI, "wss://localhost:9001/test");
        context.setCustomValue(JettyWebSocketClient.PROXY_HOST, "localhost");
        service.initialize(context.getInitializationContext());
        final Collection<ValidationResult> results = service.validate(context.getValidationContext());
        assertEquals(1, results.size());
        final ValidationResult result = results.iterator().next();
        assertTrue(result.getSubject().contains("Proxy"));
    }

    @Test
    public void testValidationProxyPortOnly() throws Exception {
        final JettyWebSocketClient service = new JettyWebSocketClient();
        final ControllerServiceTestContext context = new ControllerServiceTestContext(service, "service-id");
        context.setCustomValue(JettyWebSocketClient.WS_URI, "wss://localhost:9001/test");
        context.setCustomValue(JettyWebSocketClient.PROXY_PORT, "3128");
        service.initialize(context.getInitializationContext());
        final Collection<ValidationResult> results = service.validate(context.getValidationContext());
        assertEquals(1, results.size());
        final ValidationResult result = results.iterator().next();
        assertTrue(result.getSubject().contains("Proxy"));
    }

    @Test
    public void testValidationSuccessWithProxy() throws Exception {
        final JettyWebSocketClient service = new JettyWebSocketClient();
        final ControllerServiceTestContext context = new ControllerServiceTestContext(service, "service-id");
        context.setCustomValue(JettyWebSocketClient.WS_URI, "wss://localhost:9001/test");
        context.setCustomValue(JettyWebSocketClient.PROXY_HOST, "localhost");
        context.setCustomValue(JettyWebSocketClient.PROXY_PORT, "3128");
        service.initialize(context.getInitializationContext());
        final Collection<ValidationResult> results = service.validate(context.getValidationContext());
        assertEquals(0, results.size());
    }

    @Test
    public void testCustomAuthHeader() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));
        final TestableJettyWebSocketClient testSubject = new TestableJettyWebSocketClient();
        runner.addControllerService("client", testSubject);
        runner.setProperty(testSubject, JettyWebSocketClient.WS_URI, "wss://localhost:9001/test");
        runner.setProperty(testSubject, JettyWebSocketClient.CUSTOM_AUTH, CUSTOM_AUTH);
        runner.assertValid(testSubject);
        runner.enableControllerService(testSubject);
        assertEquals(CUSTOM_AUTH, testSubject.getAuthHeaderValue());
    }

    private static class TestableJettyWebSocketClient extends JettyWebSocketClient {
        public String getAuthHeaderValue() {
            return authorizationHeader;
        }
    }
}
