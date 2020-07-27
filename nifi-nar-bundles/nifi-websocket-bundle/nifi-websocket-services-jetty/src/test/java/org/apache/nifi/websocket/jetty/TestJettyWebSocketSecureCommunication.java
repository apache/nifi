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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.websocket.WebSocketService;
import org.junit.Test;


public class TestJettyWebSocketSecureCommunication extends TestJettyWebSocketCommunication{

    private final StandardSSLContextService sslContextService = new StandardSSLContextService();
    private final ControllerServiceTestContext sslTestContext = new ControllerServiceTestContext(sslContextService, "SSLContextService");

    public TestJettyWebSocketSecureCommunication() {
        try {
            sslTestContext.setCustomValue(StandardSSLContextService.KEYSTORE, "src/test/resources/certs/localhost-ks.jks");
            sslTestContext.setCustomValue(StandardSSLContextService.KEYSTORE_PASSWORD, "localtest");
            sslTestContext.setCustomValue(StandardSSLContextService.KEYSTORE_TYPE, "JKS");
            sslTestContext.setCustomValue(StandardSSLContextService.TRUSTSTORE, "src/test/resources/certs/localhost-ks.jks");
            sslTestContext.setCustomValue(StandardSSLContextService.TRUSTSTORE_PASSWORD, "localtest");
            sslTestContext.setCustomValue(StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");

            sslContextService.initialize(sslTestContext.getInitializationContext());
            sslContextService.onConfigured(sslTestContext.getConfigurationContext());
        } catch (InitializationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean isSecure() {
        return true;
    }

    @Override
    protected void customizeServer() {
        serverServiceContext.getInitializationContext().addControllerService(sslContextService);
        serverServiceContext.setCustomValue(WebSocketService.SSL_CONTEXT, sslContextService.getIdentifier());
    }

    @Override
    protected void customizeClient() {
        clientServiceContext.getInitializationContext().addControllerService(sslContextService);
        clientServiceContext.setCustomValue(WebSocketService.SSL_CONTEXT, sslContextService.getIdentifier());
    }

    @Test
    public void testClientServerCommunication() throws Exception {
        super.testClientServerCommunication();
    }

}
