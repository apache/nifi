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

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.models.LastEnqueuedEventProperties;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.azure.eventhub.utils.AzureEventHubUtils;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubAuthenticationStrategy;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubTransportType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.proxy.ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class GetAzureEventHubTest {
    private static final String DOMAIN_NAME = "DOMAIN";
    private static final String EVENT_HUB_NAMESPACE = "NAMESPACE";
    private static final String EVENT_HUB_NAME = "NAME";
    private static final String POLICY_NAME = "POLICY";
    private static final String POLICY_KEY = "POLICY-KEY";
    private static final String CONSUMER_GROUP = "$Default";
    private static final String EVENT_HUB_OAUTH_SERVICE_ID = "get-event-hub-oauth";
    private static final Instant ENQUEUED_TIME = Instant.now();
    private static final long SEQUENCE_NUMBER = 32;
    private static final String OFFSET = "64";
    private static final String PARTITION_ID = "0";
    private static final String CONTENT = String.class.getSimpleName();

    private List<PartitionEvent> partitionEvents;

    private TestRunner testRunner;

    @BeforeEach
    public void setUp() {
        partitionEvents = new ArrayList<>();
        testRunner = TestRunners.newTestRunner(new MockGetAzureEventHub());
    }

    @Test
    public void testProperties() throws InitializationException {
        testRunner.setProperty(GetAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.AUTHENTICATION_STRATEGY, AzureEventHubAuthenticationStrategy.SHARED_ACCESS_SIGNATURE.getValue());
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.ACCESS_POLICY, POLICY_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.ENQUEUE_TIME, ENQUEUED_TIME.toString());
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.RECEIVER_FETCH_SIZE, "5");
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.RECEIVER_FETCH_TIMEOUT, "10000");
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.TRANSPORT_TYPE, AzureEventHubTransportType.AMQP_WEB_SOCKETS);
        testRunner.assertValid();
        configureProxyControllerService();
        testRunner.assertValid();
    }

    @Test
    void testMigration() {
        TestRunner testRunner = TestRunners.newTestRunner(GetAzureEventHub.class);
        final PropertyMigrationResult propertyMigrationResult = testRunner.migrateProperties();
        final Map<String, String> expected = Map.ofEntries(
                Map.entry("Event Hub Consumer Group", GetAzureEventHub.CONSUMER_GROUP.getName()),
                Map.entry("Event Hub Message Enqueue Time", GetAzureEventHub.ENQUEUE_TIME.getName()),
                Map.entry("Partition Recivier Fetch Size", GetAzureEventHub.RECEIVER_FETCH_SIZE.getName()),
                Map.entry("Partition Receiver Timeout (millseconds)", GetAzureEventHub.RECEIVER_FETCH_TIMEOUT.getName()),
                Map.entry(AzureEventHubUtils.OLD_POLICY_PRIMARY_KEY_DESCRIPTOR_NAME, GetAzureEventHub.POLICY_PRIMARY_KEY.getName()),
                Map.entry(AzureEventHubUtils.OLD_USE_MANAGED_IDENTITY_DESCRIPTOR_NAME, AzureEventHubUtils.LEGACY_USE_MANAGED_IDENTITY_PROPERTY_NAME),
                Map.entry(ProxyServiceMigration.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyServiceMigration.PROXY_CONFIGURATION_SERVICE)
        );

        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());
    }

    @Test
    void testMigrationPreservesSharedAccessAuthentication() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(AzureEventHubUtils.LEGACY_USE_MANAGED_IDENTITY_PROPERTY_NAME, "false");
        properties.put(GetAzureEventHub.ACCESS_POLICY.getName(), POLICY_NAME);
        properties.put(GetAzureEventHub.POLICY_PRIMARY_KEY.getName(), POLICY_KEY);
        properties.put(GetAzureEventHub.AUTHENTICATION_STRATEGY.getName(), AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY.getValue());

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        new GetAzureEventHub().migrateProperties(configuration);

        assertEquals(AzureEventHubAuthenticationStrategy.SHARED_ACCESS_SIGNATURE.getValue(),
                configuration.getRawProperties().get(GetAzureEventHub.AUTHENTICATION_STRATEGY.getName()));
    }

    @Test
    void testMigrationDoesNotOverrideExplicitAuthenticationStrategy() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(GetAzureEventHub.AUTHENTICATION_STRATEGY.getName(), AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY.getValue());
        properties.put(GetAzureEventHub.ACCESS_POLICY.getName(), POLICY_NAME);
        properties.put(GetAzureEventHub.POLICY_PRIMARY_KEY.getName(), POLICY_KEY);

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        new GetAzureEventHub().migrateProperties(configuration);

        assertEquals(AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY.getValue(),
                configuration.getRawProperties().get(GetAzureEventHub.AUTHENTICATION_STRATEGY.getName()));
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
        testRunner.setProperty(GetAzureEventHub.EVENT_HUB_OAUTH2_ACCESS_TOKEN_PROVIDER, EVENT_HUB_OAUTH_SERVICE_ID);
    }

    @Test
    public void testPropertiesManagedIdentity() {
        testRunner.setProperty(GetAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.AUTHENTICATION_STRATEGY, AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY.getValue());
        testRunner.assertValid();
    }

    @Test
    public void testEventHubOAuthRequiresTokenProvider() throws InitializationException {
        testRunner.setProperty(GetAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.setProperty(GetAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.setProperty(GetAzureEventHub.AUTHENTICATION_STRATEGY, AzureEventHubAuthenticationStrategy.OAUTH2.getValue());

        testRunner.assertNotValid();

        configureEventHubOAuthTokenProvider();

        testRunner.assertValid();
    }

    @Test
    public void testRunNoEventsReceived() {
        setProperties();

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 0);
    }

    @Test
    public void testRunEventsReceived() {
        setProperties();

        final PartitionEvent partitionEvent = createPartitionEvent();
        partitionEvents.add(partitionEvent);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GetAzureEventHub.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(CONTENT);
        flowFile.assertAttributeEquals("eventhub.enqueued.timestamp", ENQUEUED_TIME.toString());
        flowFile.assertAttributeEquals("eventhub.offset", OFFSET);
        flowFile.assertAttributeEquals("eventhub.sequence", Long.toString(SEQUENCE_NUMBER));
        flowFile.assertAttributeEquals("eventhub.name", EVENT_HUB_NAME);
    }

    @Test
    public void testPrimaryNodeRevoked() {
        setProperties();

        final ProcessContext processContext = spy(testRunner.getProcessContext());
        when(processContext.getExecutionNode()).thenReturn(ExecutionNode.PRIMARY);

        testRunner.setIsConfiguredForClustering(true);
        testRunner.setPrimaryNode(true);
        final GetAzureEventHub processor = (GetAzureEventHub) testRunner.getProcessor();
        processor.onScheduled(processContext);
        processor.onPrimaryNodeStateChange(PrimaryNodeState.PRIMARY_NODE_REVOKED);

        final PartitionEvent partitionEvent = createPartitionEvent();
        partitionEvents.add(partitionEvent);

        testRunner.run(1, true, false);
        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 0);
    }

    @Test
    public void testPrimaryNodeRevokedThenElected() {
        setProperties();

        final ProcessContext processContext = spy(testRunner.getProcessContext());
        when(processContext.getExecutionNode()).thenReturn(ExecutionNode.PRIMARY);

        testRunner.setIsConfiguredForClustering(true);
        testRunner.setPrimaryNode(true);
        final GetAzureEventHub processor = (GetAzureEventHub) testRunner.getProcessor();
        processor.onScheduled(processContext);
        processor.onPrimaryNodeStateChange(PrimaryNodeState.PRIMARY_NODE_REVOKED);
        processor.onPrimaryNodeStateChange(PrimaryNodeState.ELECTED_PRIMARY_NODE);

        final PartitionEvent partitionEvent = createPartitionEvent();
        partitionEvents.add(partitionEvent);

        testRunner.run(1, true, false);
        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 1);
    }

    private class MockGetAzureEventHub extends GetAzureEventHub {

        @Override
        protected BlockingQueue<String> getPartitionIds() {
            return new LinkedBlockingQueue<>(Collections.singleton(PARTITION_ID));
        }

        @Override
        protected Iterable<PartitionEvent> receiveEvents(final String partitionId) {
            return partitionEvents;
        }
    }

    private PartitionEvent createPartitionEvent() {
        final PartitionContext partitionContext = new PartitionContext(DOMAIN_NAME, EVENT_HUB_NAME, CONSUMER_GROUP, PARTITION_ID);
        final EventData eventData = new MockEventData();

        final LastEnqueuedEventProperties lastEnqueuedEventProperties = new LastEnqueuedEventProperties(SEQUENCE_NUMBER, OFFSET, ENQUEUED_TIME, ENQUEUED_TIME);
        return new PartitionEvent(partitionContext, eventData, lastEnqueuedEventProperties);
    }

    private void setProperties() {
        testRunner.setProperty(GetAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.setProperty(GetAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.setProperty(GetAzureEventHub.AUTHENTICATION_STRATEGY, AzureEventHubAuthenticationStrategy.SHARED_ACCESS_SIGNATURE.getValue());
        testRunner.setProperty(GetAzureEventHub.ACCESS_POLICY, POLICY_NAME);
        testRunner.setProperty(GetAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertValid();
    }

    private static class MockEventData extends EventData {
        private MockEventData() {
            super(CONTENT);
        }

        @Override
        public String getOffsetString() {
            return OFFSET;
        }

        @Override
        public Long getSequenceNumber() {
            return SEQUENCE_NUMBER;
        }

        @Override
        public Instant getEnqueuedTime() {
            return ENQUEUED_TIME;
        }
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
