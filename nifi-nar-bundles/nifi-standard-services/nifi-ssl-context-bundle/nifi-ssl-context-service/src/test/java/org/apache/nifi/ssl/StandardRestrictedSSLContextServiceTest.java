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

import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsPlatform;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class StandardRestrictedSSLContextServiceTest {

    private static final String SERVICE_ID = StandardRestrictedSSLContextService.class.getSimpleName();

    private static TlsConfiguration tlsConfiguration;

    @Mock
    private Processor processor;

    private StandardRestrictedSSLContextService service;

    private TestRunner runner;

    @BeforeAll
    public static void setConfiguration() {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build();
    }

    @BeforeEach
    public void setRunner() {
        runner = TestRunners.newTestRunner(processor);
        service = new StandardRestrictedSSLContextService();
    }

    @Test
    public void testMinimumPropertiesValid() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        setMinimumProperties();
        runner.assertValid(service);
    }

    @Test
    public void testPreferredProtocolsValid() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        setMinimumProperties();

        for (final String protocol : TlsPlatform.getPreferredProtocols()) {
            runner.setProperty(service, StandardRestrictedSSLContextService.SSL_ALGORITHM, protocol);
            runner.assertValid(service);
        }
    }

    @Test
    public void testPreferredProtocolsCreateContext() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        setMinimumProperties();

        for (final String protocol : TlsPlatform.getPreferredProtocols()) {
            runner.setProperty(service, StandardRestrictedSSLContextService.SSL_ALGORITHM, protocol);
            runner.assertValid(service);
            runner.enableControllerService(service);

            final SSLContext sslContext = service.createContext();
            assertEquals(protocol, sslContext.getProtocol());

            runner.disableControllerService(service);
        }
    }

    private void setMinimumProperties() {
        runner.setProperty(service, StandardRestrictedSSLContextService.KEYSTORE, tlsConfiguration.getKeystorePath());
        runner.setProperty(service, StandardRestrictedSSLContextService.KEYSTORE_PASSWORD, tlsConfiguration.getKeystorePassword());
        runner.setProperty(service, StandardRestrictedSSLContextService.KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        runner.setProperty(service, StandardRestrictedSSLContextService.TRUSTSTORE, tlsConfiguration.getTruststorePath());
        runner.setProperty(service, StandardRestrictedSSLContextService.TRUSTSTORE_PASSWORD, tlsConfiguration.getTruststorePassword());
        runner.setProperty(service, StandardRestrictedSSLContextService.TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
    }
}
