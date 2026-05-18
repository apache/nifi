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
package org.apache.nifi.kafka.processors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.nifi.components.Backlog;
import org.apache.nifi.components.BacklogReportingException;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.Kafka3ConnectionService;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link ConsumeKafka#getBacklog(org.apache.nifi.processor.ProcessContext)} backed by a Testcontainers Kafka
 * broker. Covers the {@link Backlog} payload reported for non-zero lag, the caught-up shape
 * after a full drain, the persistence of {@code lastCaughtUp} across subsequent re-lagged
 * reports, and pattern-based subscriptions.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class ConsumeKafkaBacklogIT extends AbstractConsumeKafkaIT {

    private static final String MESSAGE_VALUE = "abcdefghij";

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);
        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());
    }

    @Test
    void testBacklogReportsLagBeforeDrain() throws Exception {
        final String topic = newTopic();
        configureProcessor(topic);
        configurePartialDrain();

        final int seedCount = 5;
        seedMessages(topic, seedCount);

        runner.run(1, false, true);
        while (runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()) {
            runner.run(1, false, false);
        }

        final int delivered = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).size();
        assertTrue(delivered > 0 && delivered < seedCount, "Partial drain expected; delivered=" + delivered);

        final Backlog backlog = requireBacklog();
        assertEquals(Backlog.Precision.EXACT, backlog.getPrecision());
        assertTrue(backlog.getRecordCount().isPresent());
        assertEquals((long) (seedCount - delivered), backlog.getRecordCount().getAsLong());
        assertFalse(backlog.getLastCaughtUp().isPresent());
    }

    @Test
    void testBacklogReportsCaughtUpAfterDrain() throws Exception {
        final String topic = newTopic();
        configureProcessor(topic);

        final int seedCount = 4;
        seedMessages(topic, seedCount);
        drainAll(seedCount);

        final Instant beforeBacklog = Instant.now();
        final Backlog backlog = requireBacklog();
        final Instant afterBacklog = Instant.now();

        assertEquals(Backlog.Precision.EXACT, backlog.getPrecision());
        // A caught-up group has an exactly-known lag of zero. The Backlog populates the records
        // dimension with that zero alongside lastCaughtUp so clients render "0 records" rather
        // than treating the record count as unknown. flowFileCount and byteCount remain omitted
        // because Kafka measures lag in records, not FlowFiles or bytes.
        assertTrue(backlog.getRecordCount().isPresent());
        assertEquals(0L, backlog.getRecordCount().getAsLong());
        assertFalse(backlog.getFlowFileCount().isPresent());
        assertFalse(backlog.getByteCount().isPresent());
        assertTrue(backlog.getLastCaughtUp().isPresent());

        final Instant lastCaughtUp = backlog.getLastCaughtUp().get();
        assertFalse(lastCaughtUp.isBefore(beforeBacklog), "lastCaughtUp=" + lastCaughtUp + " before window start=" + beforeBacklog);
        assertFalse(lastCaughtUp.isAfter(afterBacklog), "lastCaughtUp=" + lastCaughtUp + " after window end=" + afterBacklog);
    }

    @Test
    void testBacklogPreservesLastCaughtUpAfterReSeed() throws Exception {
        final String topic = newTopic();
        configureProcessor(topic);
        configurePartialDrain();

        final int initialSeed = 4;
        seedMessages(topic, initialSeed);
        drainAll(initialSeed);

        final Backlog drainedBacklog = requireBacklog();
        assertTrue(drainedBacklog.getLastCaughtUp().isPresent());
        final Instant priorCaughtUp = drainedBacklog.getLastCaughtUp().get();

        final int reSeed = 3;
        seedMessages(topic, reSeed);

        final int previouslyDelivered = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).size();
        while (runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).size() == previouslyDelivered) {
            runner.run(1, false, false);
        }

        final int redelivered = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).size() - previouslyDelivered;
        assertTrue(redelivered > 0 && redelivered < reSeed, "Partial re-drain expected; redelivered=" + redelivered);

        final Backlog reLagBacklog = requireBacklog();
        assertEquals(Backlog.Precision.EXACT, reLagBacklog.getPrecision());
        assertTrue(reLagBacklog.getRecordCount().isPresent());
        assertEquals((long) (reSeed - redelivered), reLagBacklog.getRecordCount().getAsLong());
        assertTrue(reLagBacklog.getLastCaughtUp().isPresent());
        assertSame(priorCaughtUp, reLagBacklog.getLastCaughtUp().get());
    }

    @Test
    void testBacklogWhileStoppedReportsLag() throws Exception {
        final String topic = newTopic();
        createTopic(topic, 1);
        configureProcessor(topic);

        final int seedCount = 5;
        seedMessages(topic, seedCount);

        // Never invoke @OnScheduled. getBacklog must build a fresh consumer service from the
        // supplied ProcessContext and report the stream's lag.
        final Backlog backlog = requireBacklog();
        assertEquals(Backlog.Precision.EXACT, backlog.getPrecision());
        assertTrue(backlog.getRecordCount().isPresent());
        assertEquals((long) seedCount, backlog.getRecordCount().getAsLong());
        assertFalse(backlog.getLastCaughtUp().isPresent());
    }

    @Test
    void testBacklogWhileStoppedAfterCaughtUpPreservesLastCaughtUp() throws Exception {
        final String topic = newTopic();
        configureProcessor(topic);

        final int seedCount = 3;
        seedMessages(topic, seedCount);
        drainAll(seedCount);

        // The drain call above triggered @OnScheduled and ran the processor; a caught-up moment has
        // now been recorded. Stop the processor and ask for the backlog again — the fresh Admin-client
        // query returns an exactly-known lag of zero with a lastCaughtUp timestamp populated.
        runner.run(0, true, false);

        final ConsumeKafka consumeKafka = (ConsumeKafka) runner.getProcessor();
        final Optional<Backlog> reported = consumeKafka.getBacklog(runner.getProcessContext());
        assertTrue(reported.isPresent());
        final Backlog backlog = reported.get();
        assertTrue(backlog.getLastCaughtUp().isPresent());
        assertTrue(backlog.getRecordCount().isPresent());
        assertEquals(0L, backlog.getRecordCount().getAsLong());
    }

    @Test
    void testBacklogFailsWellWithinDefaultClientTimeoutWhenBrokerUnreachable() {
        final String topic = newTopic();
        configureProcessor(topic);
        pointConnectionServiceAtUnreachableBroker();

        final ConsumeKafka consumeKafka = (ConsumeKafka) runner.getProcessor();
        final Instant beforeBacklogAttempt = Instant.now();
        assertThrows(BacklogReportingException.class, () -> consumeKafka.getBacklog(runner.getProcessContext()));
        final Duration elapsed = Duration.between(beforeBacklogAttempt, Instant.now());

        // The Admin client used for backlog determination is bounded independently of the much longer
        // default Client Timeout (60 seconds) configured for regular produce/consume operations, so an
        // unreachable broker should not stall a backlog check for anywhere close to that long.
        assertTrue(elapsed.toSeconds() < 30, "Expected backlog determination against an unreachable broker to fail in well under 30 seconds, but took " + elapsed);
    }

    private void pointConnectionServiceAtUnreachableBroker() {
        final ControllerService connectionService = runner.getControllerService(CONNECTION_SERVICE_ID);
        runner.disableControllerService(connectionService);
        runner.setProperty(connectionService, Kafka3ConnectionService.BOOTSTRAP_SERVERS, "localhost:1");
        runner.enableControllerService(connectionService);
    }

    private void configureProcessor(final String topic) {
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.GROUP_ID, "ConsumeKafkaBacklogIT-" + UUID.randomUUID());
    }

    /**
     * Constrains the inner poll loop to a single record per onTrigger so that seeded messages
     * drain in small steps, leaving observable non-zero lag for assertions.
     */
    private void configurePartialDrain() {
        final ControllerService connectionService = runner.getControllerService(CONNECTION_SERVICE_ID);
        runner.disableControllerService(connectionService);
        runner.setProperty(connectionService, Kafka3ConnectionService.MAX_POLL_RECORDS, "1");
        runner.enableControllerService(connectionService);

        runner.setProperty(ConsumeKafka.MAX_UNCOMMITTED_SIZE, "1 B");
        runner.setProperty(ConsumeKafka.MAX_UNCOMMITTED_TIME, "1 s");
    }

    private void seedMessages(final String topic, final int count) throws Exception {
        for (int index = 0; index < count; index++) {
            produceOne(topic, 0, null, MESSAGE_VALUE, null);
        }
    }

    private void drainAll(final int expectedCount) {
        runner.run(1, false, true);
        while (runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).size() < expectedCount) {
            runner.run(1, false, false);
        }
        runner.assertTransferCount(ConsumeKafka.SUCCESS, expectedCount);
    }

    private Backlog requireBacklog() throws Exception {
        final ConsumeKafka consumeKafka = (ConsumeKafka) runner.getProcessor();
        final Optional<Backlog> reported = consumeKafka.getBacklog(runner.getProcessContext());
        assertTrue(reported.isPresent());
        return reported.get();
    }

    private static String newTopic() {
        return "ConsumeKafkaBacklogIT-" + UUID.randomUUID();
    }

    private void createTopic(final String topic, final int numPartitions) throws Exception {
        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());

        try (final Admin admin = Admin.create(adminProps)) {
            final NewTopic topicCreationRequest = new NewTopic(topic, numPartitions, (short) 1);
            admin.createTopics(Collections.singletonList(topicCreationRequest)).all().get(30, TimeUnit.SECONDS);
        }
    }
}
