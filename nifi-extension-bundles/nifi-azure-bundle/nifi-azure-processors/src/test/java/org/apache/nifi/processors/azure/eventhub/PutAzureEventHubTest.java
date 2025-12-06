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
package org.apache.nifi.processors.azure.eventhub;

import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.SendOptions;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.azure.eventhub.utils.AzureEventHubUtils;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubAuthenticationStrategy;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubTransportType;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.proxy.ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PutAzureEventHubTest {
    private static final String EVENT_HUB_NAMESPACE = "NAMESPACE";
    private static final String EVENT_HUB_NAME = "NAME";
    private static final String POLICY_NAME = "POLICY";
    private static final String POLICY_KEY = "POLICY-KEY";
    private static final String PARTITION_KEY_ATTRIBUTE_NAME = "eventPartitionKey";
    private static final String PARTITION_KEY = "partition";
    private static final String CONTENT = String.class.getSimpleName();
    private static final String EVENT_HUB_OAUTH_SERVICE_ID = "put-event-hub-oauth";

    @Mock
    EventHubProducerClient eventHubProducerClient;

    @Captor
    ArgumentCaptor<SendOptions> sendOptionsArgumentCaptor;

    TestRunner testRunner;

    @BeforeEach
    public void setUp() throws Exception {
        testRunner = TestRunners.newTestRunner(new MockPutAzureEventHub());
    }

    @Test
    public void testProperties() throws InitializationException {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertValid();
        testRunner.setProperty(PutAzureEventHub.AUTHENTICATION_STRATEGY, AzureEventHubAuthenticationStrategy.SHARED_ACCESS_SIGNATURE.getValue());
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.ACCESS_POLICY, POLICY_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertValid();
        testRunner.setProperty(PutAzureEventHub.TRANSPORT_TYPE, AzureEventHubTransportType.AMQP_WEB_SOCKETS);
        testRunner.assertValid();
        configureProxyControllerService();
        testRunner.assertValid();
    }

    @Test
    void testMigration() {
        TestRunner testRunner = TestRunners.newTestRunner(PutAzureEventHub.class);
        final PropertyMigrationResult propertyMigrationResult = testRunner.migrateProperties();
        final Map<String, String> expected = Map.of(
                "partitioning-key-attribute-name", PutAzureEventHub.PARTITIONING_KEY_ATTRIBUTE_NAME.getName(),
                "max-batch-size", PutAzureEventHub.MAX_BATCH_SIZE.getName(),
                AzureEventHubUtils.OLD_POLICY_PRIMARY_KEY_DESCRIPTOR_NAME, PutAzureEventHub.POLICY_PRIMARY_KEY.getName(),
                AzureEventHubUtils.OLD_USE_MANAGED_IDENTITY_DESCRIPTOR_NAME, AzureEventHubUtils.LEGACY_USE_MANAGED_IDENTITY_PROPERTY_NAME,
                ProxyServiceMigration.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyServiceMigration.PROXY_CONFIGURATION_SERVICE
        );

        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());
    }

    @Test
    void testMigrationPreservesSharedAccessAuthentication() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(AzureEventHubUtils.LEGACY_USE_MANAGED_IDENTITY_PROPERTY_NAME, "false");
        properties.put(PutAzureEventHub.ACCESS_POLICY.getName(), POLICY_NAME);
        properties.put(PutAzureEventHub.POLICY_PRIMARY_KEY.getName(), POLICY_KEY);
        properties.put(PutAzureEventHub.AUTHENTICATION_STRATEGY.getName(), AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY.getValue());

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        new PutAzureEventHub().migrateProperties(configuration);

        assertEquals(AzureEventHubAuthenticationStrategy.SHARED_ACCESS_SIGNATURE.getValue(),
                configuration.getRawProperties().get(PutAzureEventHub.AUTHENTICATION_STRATEGY.getName()));
    }

    @Test
    void testMigrationDoesNotOverrideExplicitAuthenticationStrategy() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(PutAzureEventHub.AUTHENTICATION_STRATEGY.getName(), AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY.getValue());
        properties.put(PutAzureEventHub.ACCESS_POLICY.getName(), POLICY_NAME);
        properties.put(PutAzureEventHub.POLICY_PRIMARY_KEY.getName(), POLICY_KEY);

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        new PutAzureEventHub().migrateProperties(configuration);

        assertEquals(AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY.getValue(),
                configuration.getRawProperties().get(PutAzureEventHub.AUTHENTICATION_STRATEGY.getName()));
    }

    private void configureProxyControllerService() throws InitializationException {
        final String serviceId = "proxyConfigurationService";
        final ProxyConfiguration proxyConfiguration = mock(ProxyConfiguration.class);
        when(proxyConfiguration.getProxyType()).thenReturn(Proxy.Type.HTTP);
        final ProxyConfigurationService service = mock(ProxyConfigurationService.class);
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getConfiguration()).thenReturn(proxyConfiguration);
        testRunner.addControllerService(serviceId, service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(PROXY_CONFIGURATION_SERVICE, serviceId);
    }

    private void configureEventHubOAuthTokenProvider() throws InitializationException {
        final MockOAuth2AccessTokenProvider provider = new MockOAuth2AccessTokenProvider();
        testRunner.addControllerService(EVENT_HUB_OAUTH_SERVICE_ID, provider);
        testRunner.enableControllerService(provider);
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_OAUTH2_ACCESS_TOKEN_PROVIDER, EVENT_HUB_OAUTH_SERVICE_ID);
    }

    @Test
    public void testPropertiesManagedIdentityEnabled() {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertValid();
        testRunner.setProperty(PutAzureEventHub.AUTHENTICATION_STRATEGY, AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY.getValue());
        testRunner.assertValid();
    }

    @Test
    public void testEventHubOAuthRequiresTokenProvider() throws InitializationException {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.setProperty(PutAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.setProperty(PutAzureEventHub.AUTHENTICATION_STRATEGY, AzureEventHubAuthenticationStrategy.OAUTH2.getValue());

        testRunner.assertNotValid();

        configureEventHubOAuthTokenProvider();

        testRunner.assertValid();
    }

    @Test
    public void testRunNoFlowFiles() {
        setProperties();

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, 0);
    }

    @Test
    public void testRunSuccess() {
        setProperties();

        testRunner.enqueue(CONTENT);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, 1);
    }

    @Test
    public void testRunFailure() {
        setProperties();

        doThrow(new RuntimeException()).when(eventHubProducerClient).send(anyIterable(), any(SendOptions.class));

        testRunner.enqueue(CONTENT);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_FAILURE, 1);
    }

    @Test
    public void testRunBatchSuccess() {
        setProperties();

        final int batchSize = 2;

        testRunner.setProperty(PutAzureEventHub.MAX_BATCH_SIZE, Integer.toString(batchSize));

        testRunner.enqueue(CONTENT);
        testRunner.enqueue(CONTENT);
        testRunner.enqueue(CONTENT);
        testRunner.enqueue(CONTENT);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, batchSize);
        testRunner.clearTransferState();

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, batchSize);
    }

    @Test
    public void testRunSuccessPartitionKey() {
        setProperties();

        final Map<String, String> attributes = Collections.singletonMap(PARTITION_KEY_ATTRIBUTE_NAME, PARTITION_KEY);
        testRunner.setProperty(PutAzureEventHub.PARTITIONING_KEY_ATTRIBUTE_NAME, PARTITION_KEY_ATTRIBUTE_NAME);

        testRunner.enqueue(CONTENT, attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, 1);

        verify(eventHubProducerClient).send(anyIterable(), sendOptionsArgumentCaptor.capture());

        final SendOptions sendOptions = sendOptionsArgumentCaptor.getValue();
        assertEquals(PARTITION_KEY, sendOptions.getPartitionKey());
    }

    private class MockPutAzureEventHub extends PutAzureEventHub {
        @Override
        protected EventHubProducerClient createEventHubProducerClient(final ProcessContext context) {
            return eventHubProducerClient;
        }
    }

    private void setProperties() {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.setProperty(PutAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.setProperty(PutAzureEventHub.AUTHENTICATION_STRATEGY, AzureEventHubAuthenticationStrategy.SHARED_ACCESS_SIGNATURE.getValue());
        testRunner.setProperty(PutAzureEventHub.ACCESS_POLICY, POLICY_NAME);
        testRunner.setProperty(PutAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertValid();
    }

    private static class MockOAuth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        @Override
        public AccessToken getAccessDetails() {
            final AccessToken accessToken = new AccessToken();
            accessToken.setAccessToken("access-token");
            accessToken.setExpiresIn(TimeUnit.MINUTES.toSeconds(5));
            return accessToken;
        }
    }
}
