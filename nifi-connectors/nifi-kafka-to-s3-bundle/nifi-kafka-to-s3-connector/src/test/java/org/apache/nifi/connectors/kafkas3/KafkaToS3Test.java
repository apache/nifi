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

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.ConnectorPropertyValue;
import org.apache.nifi.components.connector.components.FlowContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link KafkaToS3}.
 *
 * <p>Tests use Mockito to stub the {@link FlowContext} and {@link ConnectorConfigurationContext}
 * surface that the connector consumes. The heavier {@code StandardConnectorTestRunner} harness
 * is not used here because it requires a packaged NAR and a running broker, which is the
 * province of the {@code nifi-kafka-to-s3-integration-tests} module.
 */
public class KafkaToS3Test {

    private ConnectorConfigurationContext overriddenContext;
    private FlowContext flowContext;

    @BeforeEach
    public void setUp() {
        overriddenContext = mock(ConnectorConfigurationContext.class);
        flowContext = mock(FlowContext.class);
    }

    /**
     * The specified-topic list passed to the topic-existence check must come from the overridden
     * {@link ConnectorConfigurationContext} supplied with the verification request, not from the
     * persisted flow configuration. This guards against silently verifying a stale topic list when
     * the request narrows the selection (for example, when the UI has dropped a topic that no
     * longer exists in the broker).
     */
    @Test
    public void verifyTopicsExistsUsesOverriddenTopicList() {
        stubSpecifiedTopics(overriddenContext, List.of("demo-topic"));

        final KafkaToS3 connector = new KafkaToS3WithAvailableTopics(List.of("demo-topic"));
        final List<ConfigVerificationResult> results = connector.verifyTopicsExists(flowContext, overriddenContext);

        assertEquals(1, results.size());
        final ConfigVerificationResult result = results.getFirst();
        assertEquals(Outcome.SUCCESSFUL, result.getOutcome());
        assertEquals("Verify Kafka topics exist", result.getVerificationStepName());
        assertTrue(result.getExplanation().contains("All specified topics exist"),
                "Explanation should indicate success, but was: " + result.getExplanation());
    }

    /**
     * A topic specified by the override that is absent from the broker must still be reported as
     * missing, ensuring the failure path is preserved for genuine misconfigurations.
     */
    @Test
    public void verifyTopicsExistsReportsTopicMissingFromBroker() {
        stubSpecifiedTopics(overriddenContext, List.of("demo-topic", "brand-new-topic"));

        final KafkaToS3 connector = new KafkaToS3WithAvailableTopics(List.of("demo-topic"));
        final List<ConfigVerificationResult> results = connector.verifyTopicsExists(flowContext, overriddenContext);

        assertEquals(1, results.size());
        final ConfigVerificationResult result = results.getFirst();
        assertEquals(Outcome.FAILED, result.getOutcome());
        assertTrue(result.getExplanation().contains("brand-new-topic"),
                "Explanation should reference the missing topic, but was: " + result.getExplanation());
    }

    /**
     * If the broker cannot be queried for available topics, the verification step is reported as
     * SKIPPED rather than FAILED, since the connector cannot determine whether the specified
     * topics exist.
     */
    @Test
    public void verifyTopicsExistsReturnsSkippedWhenAvailableTopicsLookupFails() {
        stubSpecifiedTopics(overriddenContext, List.of("demo-topic"));

        final KafkaToS3 connector = new KafkaToS3WithFailingAvailableTopics(new RuntimeException("broker unreachable"));
        final List<ConfigVerificationResult> results = connector.verifyTopicsExists(flowContext, overriddenContext);

        assertEquals(1, results.size());
        final ConfigVerificationResult result = results.getFirst();
        assertEquals(Outcome.SKIPPED, result.getOutcome());
        assertTrue(result.getExplanation().contains("broker unreachable"),
                "Explanation should propagate the lookup failure, but was: " + result.getExplanation());
    }

    private static void stubSpecifiedTopics(final ConnectorConfigurationContext context, final List<String> topics) {
        final ConnectorPropertyValue propertyValue = mock(ConnectorPropertyValue.class);
        when(propertyValue.asList()).thenReturn(topics);
        when(context.getProperty(KafkaTopicsStep.STEP_NAME,
                KafkaTopicsStep.TOPIC_NAMES.getName())).thenReturn(propertyValue);
    }

    /**
     * Test subclass that bypasses the Kafka connection service lookup and returns a canned list
     * of topics that the broker reports as available.
     */
    private static class KafkaToS3WithAvailableTopics extends KafkaToS3 {
        private final List<String> availableTopics;

        KafkaToS3WithAvailableTopics(final List<String> availableTopics) {
            this.availableTopics = availableTopics;
        }

        @Override
        List<String> getAvailableTopics(final FlowContext flowContext) {
            return availableTopics;
        }
    }

    /**
     * Test subclass that simulates a broker lookup failure so the SKIPPED outcome path is
     * exercised.
     */
    private static class KafkaToS3WithFailingAvailableTopics extends KafkaToS3 {
        private final RuntimeException failure;

        KafkaToS3WithFailingAvailableTopics(final RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        List<String> getAvailableTopics(final FlowContext flowContext) {
            throw failure;
        }
    }
}
