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
package org.apache.nifi.registry.event;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.cluster.LeaderElectionManager;
import org.apache.nifi.registry.db.DatabaseTestApplication;
import org.apache.nifi.registry.hook.Event;
import org.apache.nifi.registry.hook.EventHookException;
import org.apache.nifi.registry.hook.EventHookProvider;
import org.apache.nifi.registry.hook.EventType;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.apache.nifi.registry.provider.ProviderCreationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration tests for {@link ClusterAwareEventService} using the in-memory
 * H2 database.
 *
 * <p>This test class intentionally does NOT extend {@link org.apache.nifi.registry.db.DatabaseBaseTest}
 * (which is {@code @Transactional}).  {@link ClusterAwareEventService} delivers
 * events on a background thread; if test data were inserted inside an uncommitted
 * transaction the delivery thread would not see it.  Instead each test commits
 * data explicitly and the {@link AfterEach} cleans up.
 *
 * <p>Because {@code deliverPendingEvents()} is private, delivery is triggered
 * via {@link ClusterAwareEventService#onStartLeading()}, which submits the
 * delivery task to the already-started scheduler immediately.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = DatabaseTestApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class TestClusterAwareEventService {

    @Autowired
    private DataSource dataSource;

    private JdbcTemplate jdbcTemplate;
    private LeaderElectionManager leaderElectionManager;
    private CapturingEventHookProvider hookProvider;
    private ClusterAwareEventService eventService;

    @BeforeEach
    public void setup() {
        jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.update("DELETE FROM REGISTRY_EVENT");

        leaderElectionManager = mock(LeaderElectionManager.class);
        when(leaderElectionManager.isLeader()).thenReturn(true);

        hookProvider = new CapturingEventHookProvider();
        eventService = new ClusterAwareEventService(
                Collections.singletonList(hookProvider),
                dataSource,
                leaderElectionManager);

        // Start the delivery scheduler so onStartLeading() can submit tasks.
        eventService.start();
    }

    @AfterEach
    public void teardown() {
        eventService.destroy();
        jdbcTemplate.update("DELETE FROM REGISTRY_EVENT");
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * publish() should persist a row in REGISTRY_EVENT with PROCESSED = false.
     */
    @Test
    public void testPublishPersistsEvent() {
        final Bucket bucket = newBucket("bucket-pub");
        final Event event = EventFactory.bucketCreated(bucket);

        eventService.publish(event);

        final Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM REGISTRY_EVENT WHERE PROCESSED = FALSE",
                Integer.class);
        assertEquals(1, count, "One unprocessed event row should have been inserted");
    }

    /**
     * deliverPendingEvents (triggered via onStartLeading) should dispatch the
     * event to the hook provider and mark the row as PROCESSED = true.
     */
    @Test
    public void testDeliverPendingEventsCallsProvider() throws InterruptedException {
        // Insert and commit an unprocessed event row so the background thread can see it.
        insertRawEvent(UUID.randomUUID().toString(), "CREATE_BUCKET", buildMinimalEventJson("CREATE_BUCKET"), 0, false);

        // Set up a latch so we can wait for delivery without a fixed sleep.
        hookProvider.setExpectedDeliveries(1);

        // onStartLeading() submits delivery immediately to the scheduler thread.
        eventService.onStartLeading();

        // Wait up to 2 seconds for the delivery to complete.
        assertTrue(hookProvider.awaitDelivery(2, TimeUnit.SECONDS),
                "Provider should receive the event within 2 seconds");

        assertEquals(1, hookProvider.getEvents().size(), "Provider should have received one event");

        // Allow the scheduler task to finish writing PROCESSED = true.
        Thread.sleep(300);

        final Integer unprocessed = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM REGISTRY_EVENT WHERE PROCESSED = FALSE",
                Integer.class);
        assertEquals(0, unprocessed, "Event row should be marked PROCESSED after delivery");
    }

    /**
     * When RETRY_COUNT already equals MAX_RETRY_COUNT - 1 and the provider
     * throws, the event should be discarded (PROCESSED = true, RETRY_COUNT incremented).
     */
    @Test
    public void testMaxRetryMarksProcessed() throws InterruptedException {
        hookProvider.setShouldThrow(true);

        // Insert event already at retry count 2 (one below MAX_RETRY_COUNT = 3).
        final String eventId = UUID.randomUUID().toString();
        insertRawEvent(eventId, "CREATE_BUCKET", buildMinimalEventJson("CREATE_BUCKET"), 2, false);

        // Set a latch to detect when delivery is attempted.
        hookProvider.setExpectedDeliveries(1);

        eventService.onStartLeading();

        // Wait for the throw to be caught (latch counts down in handle() — but since we
        // throw, we can't use the latch directly).  Use a short fixed sleep instead.
        Thread.sleep(1500);

        final Integer retryCount = jdbcTemplate.queryForObject(
                "SELECT RETRY_COUNT FROM REGISTRY_EVENT WHERE EVENT_ID = ?",
                Integer.class, eventId);
        assertEquals(3, retryCount, "RETRY_COUNT should be incremented to 3");

        final Boolean processed = jdbcTemplate.queryForObject(
                "SELECT PROCESSED FROM REGISTRY_EVENT WHERE EVENT_ID = ?",
                Boolean.class, eventId);
        assertTrue(processed, "Event should be marked PROCESSED after max retries");
    }

    /**
     * When this node is not the leader the delivery task must be a no-op:
     * the provider should not be called and the event should remain unprocessed.
     */
    @Test
    public void testNonLeaderDoesNotDeliver() throws InterruptedException {
        when(leaderElectionManager.isLeader()).thenReturn(false);

        insertRawEvent(UUID.randomUUID().toString(), "CREATE_BUCKET", buildMinimalEventJson("CREATE_BUCKET"), 0, false);

        // Trigger delivery — should be a no-op because isLeader() = false.
        eventService.onStartLeading();

        // Allow time for the task to run.
        Thread.sleep(500);

        assertEquals(0, hookProvider.getEvents().size(), "Non-leader should not deliver any events");

        final Integer unprocessed = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM REGISTRY_EVENT WHERE PROCESSED = FALSE",
                Integer.class);
        assertEquals(1, unprocessed, "Event should still be unprocessed when node is not leader");
    }

    /**
     * Processed events older than RETENTION_DAYS should be deleted.
     * purgeOldEvents() runs at the end of every delivery pass.
     */
    @Test
    public void testPurgeProcessedOldEvents() throws InterruptedException {
        // Insert an old processed event (8 days ago, beyond the 7-day window).
        final Timestamp eightDaysAgo = Timestamp.from(
                Instant.now().minus(8, ChronoUnit.DAYS));
        final String oldEventId = UUID.randomUUID().toString();
        jdbcTemplate.update(
                "INSERT INTO REGISTRY_EVENT (EVENT_ID, EVENT_TYPE, EVENT_DATA, CREATED_AT, PROCESSED, RETRY_COUNT)"
                        + " VALUES (?, ?, ?, ?, TRUE, 0)",
                oldEventId, "CREATE_BUCKET", buildMinimalEventJson("CREATE_BUCKET"), eightDaysAgo);

        // Insert a recent processed event that should NOT be purged.
        final String recentEventId = UUID.randomUUID().toString();
        jdbcTemplate.update(
                "INSERT INTO REGISTRY_EVENT (EVENT_ID, EVENT_TYPE, EVENT_DATA, CREATED_AT, PROCESSED, RETRY_COUNT)"
                        + " VALUES (?, ?, ?, ?, TRUE, 0)",
                recentEventId, "DELETE_BUCKET", buildMinimalEventJson("DELETE_BUCKET"),
                Timestamp.from(Instant.now()));

        // Trigger delivery — purge runs at the end of every delivery pass.
        eventService.onStartLeading();

        // Wait for scheduler task to complete.
        Thread.sleep(1000);

        final Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM REGISTRY_EVENT WHERE EVENT_ID = ?",
                Integer.class, oldEventId);
        assertEquals(0, count, "Old processed event should have been purged");

        final Integer recentCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM REGISTRY_EVENT WHERE EVENT_ID = ?",
                Integer.class, recentEventId);
        assertEquals(1, recentCount, "Recent processed event should NOT be purged");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void insertRawEvent(final String eventId, final String eventType,
            final String eventData, final int retryCount, final boolean processed) {
        jdbcTemplate.update(
                "INSERT INTO REGISTRY_EVENT (EVENT_ID, EVENT_TYPE, EVENT_DATA, CREATED_AT, PROCESSED, RETRY_COUNT)"
                        + " VALUES (?, ?, ?, ?, ?, ?)",
                eventId, eventType, eventData,
                Timestamp.from(Instant.now()),
                processed, retryCount);
    }

    /**
     * Builds the minimal JSON payload that ClusterAwareEventService can
     * deserialize.  Includes all required fields for a CREATE_BUCKET event.
     */
    private String buildMinimalEventJson(final String eventTypeName) {
        return "{\"eventType\":\"" + eventTypeName + "\","
                + "\"fields\":[{\"name\":\"BUCKET_ID\",\"value\":\"test-bucket-id\"},"
                + "{\"name\":\"BUCKET_NAME\",\"value\":\"test\"},"
                + "{\"name\":\"BUCKET_DESCRIPTION\",\"value\":\"\"},"
                + "{\"name\":\"CREATED_TIMESTAMP\",\"value\":\"0\"},"
                + "{\"name\":\"ALLOW_PUBLIC_READ\",\"value\":\"\"},"
                + "{\"name\":\"USER\",\"value\":\"test-user\"}]}";
    }

    private static Bucket newBucket(final String name) {
        final Bucket bucket = new Bucket();
        bucket.setIdentifier(UUID.randomUUID().toString());
        bucket.setName(name);
        bucket.setCreatedTimestamp(Instant.now().toEpochMilli());
        return bucket;
    }

    // -------------------------------------------------------------------------
    // CapturingEventHookProvider
    // -------------------------------------------------------------------------

    /**
     * Simple {@link EventHookProvider} that captures handled events, optionally
     * throws on {@link #handle(Event)} to simulate delivery failure, and exposes
     * a {@link CountDownLatch} so tests can wait for expected deliveries
     * without brittle fixed-duration sleeps.
     */
    private static class CapturingEventHookProvider implements EventHookProvider {

        private final List<Event> events = new ArrayList<>();
        private volatile boolean shouldThrow = false;
        private volatile CountDownLatch latch = new CountDownLatch(0);

        @Override
        public void onConfigured(final ProviderConfigurationContext configurationContext)
                throws ProviderCreationException {
            // no-op
        }

        @Override
        public boolean shouldHandle(final EventType eventType) {
            return true;
        }

        @Override
        public void handle(final Event event) throws EventHookException {
            if (shouldThrow) {
                latch.countDown();
                throw new EventHookException("Simulated delivery failure");
            }
            events.add(event);
            latch.countDown();
        }

        /** Configures the latch to count down after {@code n} deliveries. */
        public void setExpectedDeliveries(final int n) {
            this.latch = new CountDownLatch(n);
        }

        /** Blocks until the expected deliveries complete or the timeout elapses. */
        public boolean awaitDelivery(final long timeout, final TimeUnit unit)
                throws InterruptedException {
            return latch.await(timeout, unit);
        }

        public List<Event> getEvents() {
            return events;
        }

        public void setShouldThrow(final boolean shouldThrow) {
            this.shouldThrow = shouldThrow;
        }
    }
}
