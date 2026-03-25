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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.nifi.registry.cluster.LeaderChangeListener;
import org.apache.nifi.registry.cluster.LeaderElectionManager;
import org.apache.nifi.registry.hook.Event;
import org.apache.nifi.registry.hook.EventField;
import org.apache.nifi.registry.hook.EventHookProvider;
import org.apache.nifi.registry.hook.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/**
 * Cluster-aware implementation of {@link EventService}.
 *
 * <p>Events published by <em>any</em> cluster node are persisted to the
 * {@code REGISTRY_EVENT} database table. Only the current leader node delivers
 * events to {@link EventHookProvider}s.
 *
 * <p>Delivery is performed in a background loop with retries, providing
 * at-least-once semantics for each event. If a delivery attempt fails for any
 * provider, the event may be retried and therefore re-delivered to providers
 * that have already processed it. {@link EventHookProvider} implementations
 * must be idempotent and tolerate duplicate deliveries.
 *
 * <p>The delivery loop runs every 5 seconds. Processed events are retained for
 * {@value #RETENTION_DAYS} days before being purged.
 *
 * <p>This implementation is selected when {@code nifi.registry.cluster.enabled=true}.
 * It is instantiated by {@link EventServiceConfiguration}.
 */
public class ClusterAwareEventService implements EventService, DisposableBean, LeaderChangeListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterAwareEventService.class);

    /** How often the leader polls for unprocessed events (milliseconds). */
    static final long DELIVERY_POLL_INTERVAL_MS = 5_000L;

    /** Processed events older than this many days are deleted. */
    static final int RETENTION_DAYS = 7;

    /** Maximum number of delivery attempts before an event is discarded with a WARN log. */
    static final int MAX_RETRY_COUNT = 3;

    private final JdbcTemplate jdbcTemplate;
    private final LeaderElectionManager leaderElectionManager;
    private final List<EventHookProvider> eventHookProviders;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private ScheduledExecutorService deliveryScheduler;

    public ClusterAwareEventService(final List<EventHookProvider> eventHookProviders,
            final DataSource dataSource,
            final LeaderElectionManager leaderElectionManager) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.leaderElectionManager = leaderElectionManager;
        this.eventHookProviders = new ArrayList<>(eventHookProviders);
    }

    @PostConstruct
    public void start() {
        LOGGER.info("Starting ClusterAwareEventService delivery loop.");
        deliveryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "ClusterEventDelivery");
            t.setDaemon(true);
            return t;
        });
        deliveryScheduler.scheduleWithFixedDelay(
                this::deliverPendingEvents,
                DELIVERY_POLL_INTERVAL_MS, DELIVERY_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        // Register for leader-change callbacks (no-op for DatabaseLeaderElectionManager).
        leaderElectionManager.addLeaderChangeListener(this);
    }

    // -------------------------------------------------------------------------
    // LeaderChangeListener
    // -------------------------------------------------------------------------

    @Override
    public void onStartLeading() {
        LOGGER.info("This node became cluster leader; triggering immediate event delivery.");
        // Submit to the delivery thread so we don't block the election thread
        // and avoid concurrent delivery with the scheduled poll.
        deliveryScheduler.submit(this::deliverPendingEvents);
    }

    @Override
    public void onStopLeading() {
        LOGGER.info("This node is no longer cluster leader; pausing event delivery.");
    }

    @Override
    public void destroy() {
        LOGGER.info("Shutting down ClusterAwareEventService.");
        if (deliveryScheduler != null) {
            deliveryScheduler.shutdownNow();
        }
    }

    // -------------------------------------------------------------------------
    // EventService
    // -------------------------------------------------------------------------

    @Override
    public void publish(final Event event) {
        if (event == null) {
            return;
        }

        try {
            event.validate();
        } catch (final IllegalStateException e) {
            LOGGER.error("Invalid event, discarding: {}", e.getMessage());
            return;
        }

        final String json = serializeEvent(event);
        if (json == null) {
            return;
        }

        final String eventId = UUID.randomUUID().toString();
        final Timestamp now = Timestamp.from(Instant.now());

        jdbcTemplate.update(
                "INSERT INTO REGISTRY_EVENT (EVENT_ID, EVENT_TYPE, EVENT_DATA, CREATED_AT, PROCESSED) VALUES (?, ?, ?, ?, FALSE)",
                eventId, event.getEventType().name(), json, now);

        LOGGER.debug("Persisted event {} of type {}.", eventId, event.getEventType());
    }

    // -------------------------------------------------------------------------
    // Leader-only delivery loop
    // -------------------------------------------------------------------------

    // Package-private to allow direct invocation from unit tests without reflection.
    void deliverPendingEvents() {
        if (!leaderElectionManager.isLeader()) {
            return;
        }

        try {
            final List<PersistedEvent> pending = queryPendingEvents();
            for (final PersistedEvent row : pending) {
                try {
                    final Event event = deserializeEvent(row.eventType, row.eventData);
                    if (event == null) {
                        // Malformed record: mark processed to prevent infinite retry.
                        markProcessed(row.eventId);
                        continue;
                    }
                    deliverToProviders(event);
                    markProcessed(row.eventId);
                } catch (final Exception eventException) {
                    final int newRetryCount = row.retryCount + 1;
                    incrementRetryCount(row.eventId);
                    if (newRetryCount >= MAX_RETRY_COUNT) {
                        LOGGER.warn("Discarding event after {} failed delivery attempts: id={} type={} data={}",
                                MAX_RETRY_COUNT, row.eventId, row.eventType, row.eventData);
                        markProcessed(row.eventId);
                    } else {
                        LOGGER.debug("Transient failure delivering event id={} type={} (attempt {}/{}): {}",
                                row.eventId, row.eventType, newRetryCount, MAX_RETRY_COUNT, eventException.getMessage());
                    }
                }
            }

            purgeOldEvents();
        } catch (final Exception e) {
            LOGGER.error("Error during cluster event delivery", e);
        }
    }

    private List<PersistedEvent> queryPendingEvents() {
        return jdbcTemplate.query(
                "SELECT EVENT_ID, EVENT_TYPE, EVENT_DATA, RETRY_COUNT FROM REGISTRY_EVENT WHERE PROCESSED = FALSE ORDER BY CREATED_AT",
                (rs, rowNum) -> mapRow(rs));
    }

    private PersistedEvent mapRow(final ResultSet rs) throws SQLException {
        return new PersistedEvent(
                rs.getString("EVENT_ID"),
                rs.getString("EVENT_TYPE"),
                rs.getString("EVENT_DATA"),
                rs.getInt("RETRY_COUNT"));
    }

    private void markProcessed(final String eventId) {
        jdbcTemplate.update("UPDATE REGISTRY_EVENT SET PROCESSED = TRUE WHERE EVENT_ID = ?", eventId);
    }

    private void incrementRetryCount(final String eventId) {
        jdbcTemplate.update("UPDATE REGISTRY_EVENT SET RETRY_COUNT = RETRY_COUNT + 1 WHERE EVENT_ID = ?", eventId);
    }

    private void purgeOldEvents() {
        final Timestamp cutoff = Timestamp.from(Instant.now().minus(RETENTION_DAYS, ChronoUnit.DAYS));
        final int deleted = jdbcTemplate.update(
                "DELETE FROM REGISTRY_EVENT WHERE PROCESSED = TRUE AND CREATED_AT < ?", cutoff);
        if (deleted > 0) {
            LOGGER.debug("Purged {} processed event(s) older than {} days.", deleted, RETENTION_DAYS);
        }
    }

    private void deliverToProviders(final Event event) {
        final List<String> failures = new ArrayList<>();
        for (final EventHookProvider provider : eventHookProviders) {
            try {
                if (event.getEventType() == null || provider.shouldHandle(event.getEventType())) {
                    provider.handle(event);
                }
            } catch (final Exception e) {
                LOGGER.debug("Event hook provider {} failed handling event type {}: {}",
                        provider.getClass().getSimpleName(), event.getEventType(), e.getMessage());
                failures.add(provider.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
        if (!failures.isEmpty()) {
            throw new RuntimeException(failures.size() + " event hook provider(s) failed: " + String.join("; ", failures));
        }
    }

    // -------------------------------------------------------------------------
    // JSON serialization / deserialization
    // -------------------------------------------------------------------------

    /**
     * Serializes an {@link Event} to a JSON string of the form:
     * <pre>{"eventType":"CREATE_BUCKET","fields":[{"name":"BUCKET_ID","value":"..."},...]}
     * </pre>
     */
    private String serializeEvent(final Event event) {
        try {
            final EventRecord record = new EventRecord();
            record.eventType = event.getEventType().name();
            record.fields = new ArrayList<>();
            for (final EventField field : event.getFields()) {
                final FieldRecord fr = new FieldRecord();
                fr.name = field.getName().name();
                fr.value = field.getValue();
                record.fields.add(fr);
            }
            return objectMapper.writeValueAsString(record);
        } catch (final JsonProcessingException e) {
            LOGGER.error("Failed to serialize event of type {}: {}", event.getEventType(), e.getMessage());
            return null;
        }
    }

    /**
     * Deserializes a JSON string back to a {@link StandardEvent}.
     * Returns {@code null} if the record is malformed.
     */
    private Event deserializeEvent(final String eventTypeName, final String json) {
        try {
            final EventType eventType = EventType.valueOf(eventTypeName);
            final EventRecord record = objectMapper.readValue(json, EventRecord.class);

            final StandardEvent.Builder builder = new StandardEvent.Builder().eventType(eventType);
            for (final FieldRecord fr : record.fields) {
                builder.addField(
                        org.apache.nifi.registry.hook.EventFieldName.valueOf(fr.name),
                        fr.value);
            }
            return builder.build();
        } catch (final Exception e) {
            LOGGER.error("Failed to deserialize event type='{}': {}", eventTypeName, e.getMessage());
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    static class EventRecord {
        public String eventType;
        public List<FieldRecord> fields;
    }

    static class FieldRecord {
        public String name;
        public String value;
    }

    private static class PersistedEvent {
        final String eventId;
        final String eventType;
        final String eventData;
        final int retryCount;

        PersistedEvent(final String eventId, final String eventType, final String eventData, final int retryCount) {
            this.eventId = eventId;
            this.eventType = eventType;
            this.eventData = eventData;
            this.retryCount = retryCount;
        }
    }
}
