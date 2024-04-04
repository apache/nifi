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
package org.apache.nifi.elasticsearch.unit;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.AuthorizationScheme;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl;
import org.apache.nifi.elasticsearch.TestControllerServiceProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ElasticSearchClientServiceImplTest {
    private TestRunner runner;

    private ElasticSearchClientServiceImpl service;

    private static final String HOST = "http://localhost:9200";

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new ElasticSearchClientServiceImpl();
        runner.addControllerService("Client Service", service);
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, "Client Service");
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, HOST);
    }

    @Test
    void testTransitUrl() {
        final String index = "test";
        final String type = "no-type";

        runner.setProperty(service, ElasticSearchClientService.AUTHORIZATION_SCHEME, AuthorizationScheme.NONE);
        runner.assertValid(service);
        runner.enableControllerService(service);

        assertEquals(String.format("%s/%s/%s", HOST, index, type), service.getTransitUrl(index, type));
        assertEquals(String.format("%s/%s", HOST, index), service.getTransitUrl(index, null));
    }

    @Test
    void testValidateBasicAuth() {
        runner.setProperty(service, ElasticSearchClientService.AUTHORIZATION_SCHEME, AuthorizationScheme.BASIC);
        runner.setProperty(service, ElasticSearchClientService.USERNAME, "elastic");
        runner.setProperty(service, ElasticSearchClientService.PASSWORD, "password");
        runner.assertValid(service);

        runner.removeProperty(service, ElasticSearchClientService.PASSWORD);
        assertAuthorizationPropertyValidationErrorMessage(ElasticSearchClientService.USERNAME, ElasticSearchClientService.PASSWORD);

        runner.removeProperty(service, ElasticSearchClientService.USERNAME);
        runner.assertValid(service);

        runner.setProperty(service, ElasticSearchClientService.PASSWORD, "password");
        runner.removeProperty(service, ElasticSearchClientService.USERNAME);
        assertAuthorizationPropertyValidationErrorMessage(ElasticSearchClientService.PASSWORD, ElasticSearchClientService.USERNAME);
    }

    @Test
    void testValidateApiKeyAuth() {
        runner.setProperty(service, ElasticSearchClientService.AUTHORIZATION_SCHEME, AuthorizationScheme.API_KEY);
        runner.setProperty(service, ElasticSearchClientService.API_KEY_ID, "api-key-id");
        runner.setProperty(service, ElasticSearchClientService.API_KEY, "api-key");
        runner.assertValid(service);

        runner.removeProperty(service, ElasticSearchClientService.API_KEY_ID);
        assertAuthorizationPropertyValidationErrorMessage(ElasticSearchClientService.API_KEY, ElasticSearchClientService.API_KEY_ID);

        runner.removeProperty(service, ElasticSearchClientService.API_KEY);
        runner.assertValid(service);

        runner.setProperty(service, ElasticSearchClientService.API_KEY_ID, "api-key-id");
        runner.removeProperty(service, ElasticSearchClientService.API_KEY);
        assertAuthorizationPropertyValidationErrorMessage(ElasticSearchClientService.API_KEY_ID, ElasticSearchClientService.API_KEY);
    }

    @Test
    void testValidatePkiAuth() throws InitializationException {
        runner.setProperty(service, ElasticSearchClientService.AUTHORIZATION_SCHEME, AuthorizationScheme.PKI);

        final SSLContextService sslService = mock(SSLContextService.class);
        when(sslService.getIdentifier()).thenReturn("ssl-context");
        runner.addControllerService("ssl-context", sslService);
        runner.setProperty(service, ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE, "ssl-context");
        when(sslService.isKeyStoreConfigured()).thenReturn(true);
        runner.assertValid(service);
        verify(sslService, atMostOnce()).isKeyStoreConfigured();
        reset(sslService);

        when(sslService.isKeyStoreConfigured()).thenReturn(false);
        assertPKIAuthorizationValidationErrorMessage();
        verify(sslService, atMostOnce()).isKeyStoreConfigured();
        reset(sslService);

        runner.removeProperty(service, ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE);
        assertPKIAuthorizationValidationErrorMessage();
        verify(sslService, atMostOnce()).isKeyStoreConfigured();
        reset(sslService);
    }

    private void assertAuthorizationPropertyValidationErrorMessage(final PropertyDescriptor presentProperty, final PropertyDescriptor missingProperty) {
        final AssertionFailedError afe = assertThrows(AssertionFailedError.class, () -> runner.assertValid(service));
        assertTrue(afe.getMessage().contains(String.format("if '%s' is then '%s' must be set.", presentProperty.getDisplayName(), missingProperty.getDisplayName())));
    }

    private void assertPKIAuthorizationValidationErrorMessage() {
        final AssertionFailedError afe = assertThrows(AssertionFailedError.class, () -> runner.assertValid(service));
        assertTrue(afe.getMessage().contains(String.format(
                "if '%s' is '%s' then '%s' must be set and specify a Keystore for mutual TLS encryption.",
                ElasticSearchClientService.AUTHORIZATION_SCHEME.getDisplayName(),
                AuthorizationScheme.PKI.getDisplayName(),
                ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE.getDisplayName()
        )));
    }
}
