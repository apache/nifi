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
package org.apache.nifi.schemaregistry.hortonworks;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

public class HortonworksSchemaRegistryTest {
    private HortonworksSchemaRegistry testSubject;

    private TestRunner runner;

    @Mock
    private Processor dummyProcessor;
    @Mock
    private SSLContextService mockSSLContextService;
    @Mock
    private KerberosCredentialsService mockKerberosCredentialsService;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        testSubject = new HortonworksSchemaRegistry();

        runner = TestRunners.newTestRunner(dummyProcessor);
        runner.addControllerService("hortonworks-schema-registry", testSubject);

        when(mockSSLContextService.getIdentifier()).thenReturn("ssl-controller-service-id");
        when(mockKerberosCredentialsService.getIdentifier()).thenReturn("kerberos-credentials-service-id");
    }

    @Test
    void invalidWhenBasicUsernameWithoutSSLContextIsSet() throws Exception {
        runner.setProperty(testSubject, HortonworksSchemaRegistry.URL, "http://unimportant");
        runner.setProperty(testSubject, HortonworksSchemaRegistry.BASIC_AUTH_USERNAME, "username");

        runner.assertNotValid(testSubject);
    }

    @Test
    void validWhenBasicUsernameWithSSLContextIsSet() throws Exception {
        addAndEnable(mockSSLContextService);

        runner.setProperty(testSubject, HortonworksSchemaRegistry.URL, "http://unimportant");
        runner.setProperty(testSubject, HortonworksSchemaRegistry.SSL_CONTEXT_SERVICE, mockSSLContextService.getIdentifier());
        runner.setProperty(testSubject, HortonworksSchemaRegistry.BASIC_AUTH_USERNAME, "basic username");

        runner.assertValid(testSubject);
    }

    @Test
    void invalidWhenBasicUsernameAndKerberosPrincipalBothSet() throws Exception {
        addAndEnable(mockSSLContextService);

        runner.setProperty(testSubject, HortonworksSchemaRegistry.URL, "http://unimportant");
        runner.setProperty(testSubject, HortonworksSchemaRegistry.SSL_CONTEXT_SERVICE, mockSSLContextService.getIdentifier());
        runner.setProperty(testSubject, HortonworksSchemaRegistry.BASIC_AUTH_USERNAME, "basic username");
        runner.setProperty(testSubject, HortonworksSchemaRegistry.KERBEROS_PRINCIPAL, "kerberos principal");
        runner.setProperty(testSubject, HortonworksSchemaRegistry.KERBEROS_PASSWORD, "kerberos password");

        runner.assertNotValid(testSubject);
    }

    @Test
    void invalidWhenBasicUsernameAndKerberosCredentialsServivceBothSet() throws Exception {
        addAndEnable(mockSSLContextService);
        addAndEnable(mockKerberosCredentialsService);

        runner.setProperty(testSubject, HortonworksSchemaRegistry.URL, "http://unimportant");
        runner.setProperty(testSubject, HortonworksSchemaRegistry.SSL_CONTEXT_SERVICE, mockSSLContextService.getIdentifier());
        runner.setProperty(testSubject, HortonworksSchemaRegistry.KERBEROS_CREDENTIALS_SERVICE, mockKerberosCredentialsService.getIdentifier());
        runner.setProperty(testSubject, HortonworksSchemaRegistry.BASIC_AUTH_USERNAME, "basic username");

        runner.assertNotValid(testSubject);
    }

    private void addAndEnable(ControllerService service) throws InitializationException {
        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);
    }
}
