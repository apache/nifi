/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.kafka.reporting;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestKafkaProvenanceReportingTask {

    private ReportingContext context;
    private ReportingInitializationContext initContext;
    private ConfigurationContext confContext;
    private KafkaProducerService producerService;

    private List<KafkaRecord> sentRecords;

    @BeforeEach
    void setUp() {
        context = mock(ReportingContext.class);
        initContext = mock(ReportingInitializationContext.class);
        confContext = mock(ConfigurationContext.class);
        producerService = mock(KafkaProducerService.class);
        sentRecords = new ArrayList<>();

        when(producerService.isClosed()).thenReturn(false);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked") final Iterator<KafkaRecord> it = (Iterator<KafkaRecord>) inv.getArgument(0);
            it.forEachRemaining(sentRecords::add);
            return null;
        }).when(producerService).send(any(), any());
    }

    private Map<PropertyDescriptor, String> defaultProperties() {
        final KafkaProvenanceReportingTask task = new KafkaProvenanceReportingTask();
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor pd : task.getSupportedPropertyDescriptors()) {
            properties.put(pd, pd.getDefaultValue());
        }
        properties.put(KafkaProvenanceReportingTask.INSTANCE_URL, "https://localhost:8443/nifi");
        properties.put(KafkaProvenanceReportingTask.PLATFORM, "nifi");
        properties.put(KafkaProvenanceReportingTask.TOPIC_NAME, "provenance-events");
        return properties;
    }

    private KafkaProvenanceReportingTask setup(
            final ProvenanceEventRecord event,
            final Map<PropertyDescriptor, String> properties,
            final long maxEventId) throws IOException {

        final KafkaProvenanceReportingTask task = new KafkaProvenanceReportingTask();

        // State manager
        when(context.getStateManager()).thenReturn(new MockStateManager(task));

        // ReportingContext properties (used in onTrigger)
        doAnswer(inv -> {
            final PropertyDescriptor pd = inv.getArgument(0, PropertyDescriptor.class);
            return new MockPropertyValue(properties.get(pd));
        }).when(context).getProperty(any(PropertyDescriptor.class));

        // ConfigurationContext properties (used in onScheduled)
        // KAFKA_CONNECTION_SERVICE requires a controller-service mock; everything else uses MockPropertyValue.
        final KafkaConnectionService connectionService = mock(KafkaConnectionService.class);
        when(connectionService.getProducerService(any(ProducerConfiguration.class))).thenReturn(producerService);

        doAnswer(inv -> {
            final PropertyDescriptor pd = inv.getArgument(0, PropertyDescriptor.class);
            if (KafkaProvenanceReportingTask.KAFKA_CONNECTION_SERVICE.equals(pd)) {
                final PropertyValue pv = mock(PropertyValue.class);
                doReturn(connectionService).when(pv).asControllerService(KafkaConnectionService.class);
                return pv;
            }
            return new MockPropertyValue(properties.get(pd));
        }).when(confContext).getProperty(any(PropertyDescriptor.class));

        // EventAccess: returns the test event until maxEventId total events have been delivered
        final AtomicInteger totalEvents = new AtomicInteger(0);
        final EventAccess eventAccess = mock(EventAccess.class);
        doAnswer(inv -> {
            final long startId = inv.getArgument(0, Long.class);
            final int maxRecords = inv.getArgument(1, Integer.class);
            final List<ProvenanceEventRecord> result = new ArrayList<>();
            for (int i = (int) Math.max(0, startId);
                 i < startId + maxRecords && totalEvents.get() < maxEventId;
                 i++) {
                if (event != null) {
                    result.add(event);
                }
                totalEvents.getAndIncrement();
            }
            return result;
        }).when(eventAccess).getProvenanceEvents(anyLong(), anyInt());

        // Process group hierarchy: root → processor "processor-1" / "Test Processor"
        final ProcessGroupStatus pgRoot = new ProcessGroupStatus();
        pgRoot.setId("root");
        pgRoot.setName("NiFi Flow");

        final ProcessorStatus prcRoot = new ProcessorStatus();
        prcRoot.setId("processor-1");
        prcRoot.setName("Test Processor");
        pgRoot.getProcessorStatus().add(prcRoot);

        when(eventAccess.getControllerStatus()).thenReturn(pgRoot);

        final ProvenanceEventRepository provenanceRepository = mock(ProvenanceEventRepository.class);
        doAnswer(inv -> maxEventId).when(provenanceRepository).getMaxEventId();
        when(eventAccess.getProvenanceRepository()).thenReturn(provenanceRepository);
        when(context.getEventAccess()).thenReturn(eventAccess);

        // Cluster (standalone by default; override per test for cluster scenarios)
        when(context.isClustered()).thenReturn(false);
        when(context.getClusterNodeIdentifier()).thenReturn(null);

        // Logger
        final ComponentLog logger = mock(ComponentLog.class);
        when(initContext.getIdentifier()).thenReturn("test-task-id");
        when(initContext.getLogger()).thenReturn(logger);

        return task;
    }

    private ProvenanceEventRecord createEvent() {
        return createEvent("processor-1", "TestProcessor");
    }

    private ProvenanceEventRecord createEvent(final String componentId, final String componentType) {
        final String uuid = UUID.randomUUID().toString();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("uuid", uuid);
        attributes.put("filename", "test.txt");
        attributes.put("nullAttr", null);

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(createFlowFile(1L, attributes));
        builder.setAttributes(Collections.emptyMap(), attributes);
        builder.setComponentId(componentId);
        builder.setComponentType(componentType);
        return builder.build();
    }

    private FlowFile createFlowFile(final long id, final Map<String, String> attributes) {
        final MockFlowFile flowFile = new MockFlowFile(id);
        flowFile.putAttributes(attributes);
        return flowFile;
    }

    private JsonObject parseJson(final byte[] payload) {
        try (final JsonReader reader = Json.createReader(new ByteArrayInputStream(payload))) {
            return reader.readObject();
        }
    }

    @Test
    void testJsonSerializationCoreFields() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        final ProvenanceEventRecord event = createEvent();

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size(), "Expected exactly one Kafka record");
        final JsonObject json = parseJson(sentRecords.get(0).getValue());

        // Identity fields
        assertNotNull(json.getString("eventId"), "eventId must be present");
        assertEquals("RECEIVE", json.getString("eventType"));
        assertNotNull(json.getJsonNumber("eventOrdinal"), "eventOrdinal must be present");
        assertNotNull(json.getJsonNumber("timestampMillis"), "timestampMillis must be present");
        assertNotNull(json.getString("timestamp"), "timestamp (ISO-8601) must be present");
        assertNotNull(json.getJsonNumber("durationMillis"), "durationMillis must be present");

        // Component fields
        assertEquals("processor-1", json.getString("componentId"));
        assertEquals("TestProcessor", json.getString("componentType"));
        assertEquals("Test Processor", json.getString("componentName"),
                "componentName resolved from process group status");
        assertEquals("org.apache.nifi.flowfile.FlowFile", json.getString("entityType"));

        // Platform / application
        assertEquals("nifi", json.getString("platform"));
        assertEquals("NiFi Flow", json.getString("application"),
                "application is the root process group name");
    }

    @Test
    void testJsonTimestampIsIso8601() throws Exception {
        final KafkaProvenanceReportingTask task = setup(createEvent(), defaultProperties(), 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final String timestamp = parseJson(sentRecords.get(0).getValue()).getString("timestamp");
        // Must parse as a valid instant (e.g. "2025-04-28T10:30:00.000Z")
        assertDoesNotThrow(() -> Instant.parse(timestamp),
                "timestamp must be a parseable ISO-8601 instant: " + timestamp);
        assertTrue(timestamp.endsWith("Z"), "timestamp must be UTC (ends with Z)");
    }

    @Test
    void testJsonNullValuesOmittedByDefault() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.ALLOW_NULL_VALUES, "false");

        // Event has no 'details' set; it will be null in the record
        final KafkaProvenanceReportingTask task = setup(createEvent(), props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue());

        assertFalse(json.containsKey("details"),
                "Null 'details' field must be omitted when allowNullValues=false");
        assertFalse(json.containsKey("remoteIdentifier"),
                "Null 'remoteIdentifier' field must be omitted when allowNullValues=false");
        assertFalse(json.containsKey("alternateIdentifier"),
                "Null 'alternateIdentifier' field must be omitted when allowNullValues=false");
    }

    @Test
    void testJsonNullValuesIncludedWhenEnabled() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.ALLOW_NULL_VALUES, "true");

        final KafkaProvenanceReportingTask task = setup(createEvent(), props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue());

        assertEquals(JsonValue.NULL, json.get("details"),
                "Null 'details' field must appear as JSON null when allowNullValues=true");
        assertEquals(JsonValue.NULL, json.get("remoteIdentifier"),
                "Null 'remoteIdentifier' must appear as JSON null when allowNullValues=true");
    }

    @Test
    void testJsonUpdatedAttributesSerialized() throws Exception {
        final KafkaProvenanceReportingTask task = setup(createEvent(), defaultProperties(), 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue());
        final JsonObject attrs = json.getJsonObject("updatedAttributes");

        assertNotNull(attrs, "updatedAttributes must be present");
        assertEquals("test.txt", attrs.getString("filename"));
        // null-valued attribute "nullAttr" omitted because allowNullValues defaults to false
        assertFalse(attrs.containsKey("nullAttr"));
    }

    @Test
    void testMessageKeyLineageStart() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.MESSAGE_KEY_FIELD,
                KafkaProvenanceReportingTask.KEY_LINEAGE_START.getValue());

        final KafkaProvenanceReportingTask task = setup(createEvent(), props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final byte[] key = sentRecords.get(0).getKey();
        assertNotNull(key, "Message key must be set for lineageStart");
        // Key must be parseable as a long (epoch millis)
        assertDoesNotThrow(() -> Long.parseLong(new String(key, StandardCharsets.UTF_8)),
                "lineageStart key must be a numeric epoch-millis string");
    }

    @Test
    void testMessageKeyEntityId() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.MESSAGE_KEY_FIELD,
                KafkaProvenanceReportingTask.KEY_ENTITY_ID.getValue());

        final ProvenanceEventRecord event = createEvent();
        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final String key = new String(sentRecords.get(0).getKey(), StandardCharsets.UTF_8);
        assertEquals(event.getFlowFileUuid(), key, "Key must equal the FlowFile UUID");
    }

    @Test
    void testMessageKeyNone() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.MESSAGE_KEY_FIELD,
                KafkaProvenanceReportingTask.KEY_NONE.getValue());

        final KafkaProvenanceReportingTask task = setup(createEvent(), props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        assertNull(sentRecords.get(0).getKey(), "No key must be set when KEY_NONE is selected");
    }

    @Test
    void testFilterIncludeEventTypeAllowsMatchingEvents() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_EVENT_TYPE, "RECEIVE");

        final KafkaProvenanceReportingTask task = setup(createEvent(), props, 3);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(3, sentRecords.size(),
                "All RECEIVE events must pass when RECEIVE is the include filter");
    }

    @Test
    void testFilterIncludeEventTypeFiltersOutNonMatchingEvents() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_EVENT_TYPE, "DROP");

        final KafkaProvenanceReportingTask task = setup(createEvent(), props, 3);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, sentRecords.size(),
                "RECEIVE events must be filtered out when only DROP is included");
    }

    @Test
    void testFilterExcludeEventTypeFiltersOutMatchingEvents() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_EVENT_TYPE_EXCLUDE, "RECEIVE");

        final KafkaProvenanceReportingTask task = setup(createEvent(), props, 3);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, sentRecords.size(),
                "RECEIVE events must be excluded when RECEIVE is in the exclude filter");
    }

    @Test
    void testFilterExcludeTakesPrecedenceOverInclude() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_EVENT_TYPE, "RECEIVE");
        props.put(KafkaProvenanceReportingTask.FILTER_EVENT_TYPE_EXCLUDE, "RECEIVE");

        final KafkaProvenanceReportingTask task = setup(createEvent(), props, 3);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, sentRecords.size(),
                "Exclude filter must take precedence over include filter");
    }

    @Test
    void testFilterIncludeComponentTypeAllowsMatchingEvents() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_COMPONENT_TYPE, "Test.*");

        final KafkaProvenanceReportingTask task = setup(createEvent("processor-1", "TestProcessor"), props, 2);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(2, sentRecords.size(),
                "Events from 'TestProcessor' must pass the 'Test.*' include regex");
    }

    @Test
    void testFilterIncludeComponentTypeFiltersOutNonMatchingEvents() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_COMPONENT_TYPE, "SomeOther.*");

        final KafkaProvenanceReportingTask task = setup(createEvent("processor-1", "TestProcessor"), props, 2);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, sentRecords.size(),
                "Events from 'TestProcessor' must be filtered out by the 'SomeOther.*' include regex");
    }

    @Test
    void testFilterExcludeComponentTypeFiltersOutMatchingEvents() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_COMPONENT_TYPE_EXCLUDE, "Test.*");

        final KafkaProvenanceReportingTask task = setup(createEvent("processor-1", "TestProcessor"), props, 2);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, sentRecords.size(),
                "Events from 'TestProcessor' must be excluded by the 'Test.*' exclude regex");
    }

    @Test
    void testFilterIncludeComponentIdAllowsMatchingEvents() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_COMPONENT_ID, "processor-1");

        final KafkaProvenanceReportingTask task = setup(createEvent("processor-1", "TestProcessor"), props, 2);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(2, sentRecords.size(),
                "Events from 'processor-1' must pass when it is the include ID");
    }

    @Test
    void testFilterIncludeComponentIdFiltersOutNonMatchingEvents() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_COMPONENT_ID, "other-processor");

        final KafkaProvenanceReportingTask task = setup(createEvent("processor-1", "TestProcessor"), props, 2);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, sentRecords.size(),
                "Events from 'processor-1' must be filtered out when 'other-processor' is included");
    }

    @Test
    void testFilterExcludeComponentIdFiltersOutMatchingEvents() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_COMPONENT_ID_EXCLUDE, "processor-1");

        final KafkaProvenanceReportingTask task = setup(createEvent("processor-1", "TestProcessor"), props, 2);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, sentRecords.size(),
                "Events from 'processor-1' must be excluded when it is in the exclude ID list");
    }

    @Test
    void testSkipTriggerWhenClusteredWithoutNodeId() throws Exception {
        final KafkaProvenanceReportingTask task = setup(createEvent(), defaultProperties(), 1);
        task.initialize(initContext);
        task.onScheduled(confContext);

        // Simulate clustered node that hasn't received its node ID yet
        when(context.isClustered()).thenReturn(true);
        when(context.getClusterNodeIdentifier()).thenReturn(null);
        task.onTrigger(context);

        assertEquals(0, sentRecords.size(),
                "Trigger must be skipped when running clustered but node ID is not yet available");
    }

    @Test
    void testSkipTriggerWhenProducerClosed() throws Exception {
        // Signal the producer as closed before it is used
        when(producerService.isClosed()).thenReturn(true);

        final KafkaProvenanceReportingTask task = setup(createEvent(), defaultProperties(), 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, sentRecords.size(),
                "Trigger must be skipped when the KafkaProducerService is closed");
    }

    @Test
    void testContentUriPresentWhenInstanceUrlConfigured() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.INSTANCE_URL, "https://localhost:8443/nifi");

        final KafkaProvenanceReportingTask task = setup(createEvent(), props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue());

        assertTrue(json.containsKey("contentURI"), "contentURI must be present");
        assertTrue(json.containsKey("previousContentURI"), "previousContentURI must be present");
        assertTrue(json.getString("contentURI").contains("/nifi-api/provenance-events/"),
                "contentURI must contain the NiFi API path");
        assertTrue(json.getString("contentURI").endsWith("/content/output"),
                "contentURI must end with /content/output");
        assertTrue(json.getString("previousContentURI").endsWith("/content/input"),
                "previousContentURI must end with /content/input");
    }

    @Test
    void testContentUriAbsentWhenInstanceUrlBlank() throws Exception {
        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.INSTANCE_URL, "");
        props.put(KafkaProvenanceReportingTask.ALLOW_NULL_VALUES, "false");

        final KafkaProvenanceReportingTask task = setup(createEvent(), props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue());

        assertFalse(json.containsKey("contentURI"),
                "contentURI must be absent when no instance URL is configured");
        assertFalse(json.containsKey("previousContentURI"),
                "previousContentURI must be absent when no instance URL is configured");
    }

    @Test
    void testCustomValidateBothWhitelistAndBlacklistFails() {
        final KafkaProvenanceReportingTask task = new KafkaProvenanceReportingTask();
        final ValidationContext validationContext = mock(ValidationContext.class);
        doAnswer(inv -> {
            final PropertyDescriptor pd = inv.getArgument(0, PropertyDescriptor.class);
            if (KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_INCLUDE.equals(pd)) {
                return new MockPropertyValue("foo");
            }
            if (KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_EXCLUDE.equals(pd)) {
                return new MockPropertyValue("bar");
            }
            return new MockPropertyValue(null);
        }).when(validationContext).getProperty(any(PropertyDescriptor.class));

        final Collection<ValidationResult> results = task.customValidate(validationContext);
        assertFalse(results.isEmpty(),
                "customValidate must return at least one result when both whitelist and blacklist are set");
        assertTrue(results.stream().anyMatch(r -> !r.isValid()),
                "At least one validation result must indicate failure when both filters are configured");
    }

    @Test
    void testCustomValidateOnlyWhitelistIsValid() {
        final KafkaProvenanceReportingTask task = new KafkaProvenanceReportingTask();
        final ValidationContext validationContext = mock(ValidationContext.class);
        doAnswer(inv -> {
            final PropertyDescriptor pd = inv.getArgument(0, PropertyDescriptor.class);
            if (KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_INCLUDE.equals(pd)) {
                return new MockPropertyValue("foo");
            }
            return new MockPropertyValue(null);
        }).when(validationContext).getProperty(any(PropertyDescriptor.class));

        final Collection<ValidationResult> results = task.customValidate(validationContext);
        assertTrue(results.isEmpty() || results.stream().allMatch(ValidationResult::isValid),
                "customValidate must pass when only whitelist is configured");
    }

    @Test
    void testCustomValidateOnlyBlacklistIsValid() {
        final KafkaProvenanceReportingTask task = new KafkaProvenanceReportingTask();
        final ValidationContext validationContext = mock(ValidationContext.class);
        doAnswer(inv -> {
            final PropertyDescriptor pd = inv.getArgument(0, PropertyDescriptor.class);
            if (KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_EXCLUDE.equals(pd)) {
                return new MockPropertyValue("bar");
            }
            return new MockPropertyValue(null);
        }).when(validationContext).getProperty(any(PropertyDescriptor.class));

        final Collection<ValidationResult> results = task.customValidate(validationContext);
        assertTrue(results.isEmpty() || results.stream().allMatch(ValidationResult::isValid),
                "customValidate must pass when only blacklist is configured");
    }

    private ProvenanceEventRecord createEventWithAttributes(
            final Map<String, String> previousAttributes,
            final Map<String, String> updatedAttributes) {
        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(createFlowFile(1L, updatedAttributes));
        builder.setAttributes(previousAttributes, updatedAttributes);
        builder.setComponentId("processor-1");
        builder.setComponentType("TestProcessor");
        return builder.build();
    }

    @Test
    void testAttributeWhitelistLiteralNamesIncludesOnlyMatchingAttributes() throws Exception {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("foo", "foo-value");
        attrs.put("bar", "bar-value");
        attrs.put("baz", "baz-value");
        final ProvenanceEventRecord event = createEventWithAttributes(attrs, attrs);

        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_INCLUDE, "foo,bar");

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertFalse(task.attributeIncludePatterns.get().isEmpty(), "Whitelist patterns must be compiled in onScheduled");
        assertEquals(2, task.attributeIncludePatterns.get().size(), "Must have exactly 2 compiled patterns");

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue()).getJsonObject("updatedAttributes");
        assertTrue(json.containsKey("foo"), "Whitelisted attribute 'foo' must be present");
        assertTrue(json.containsKey("bar"), "Whitelisted attribute 'bar' must be present");
        assertFalse(json.containsKey("baz"), "Non-whitelisted attribute 'baz' must be absent");
    }

    @Test
    void testAttributeWhitelistRegexPatternIncludesMatchingAttributes() throws Exception {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("user.id", "u-001");
        attrs.put("user.name", "alice");
        attrs.put("filename", "data.csv");
        final ProvenanceEventRecord event = createEventWithAttributes(attrs, attrs);

        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_INCLUDE, "user\\..*");

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue()).getJsonObject("updatedAttributes");
        assertTrue(json.containsKey("user.id"), "Attribute matching regex must be included");
        assertTrue(json.containsKey("user.name"), "Attribute matching regex must be included");
        assertFalse(json.containsKey("filename"), "Non-matching attribute must be excluded");
    }

    @Test
    void testAttributeBlacklistLiteralNamesExcludesMatchingAttributes() throws Exception {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("foo", "foo-value");
        attrs.put("bar", "bar-value");
        attrs.put("baz", "baz-value");
        final ProvenanceEventRecord event = createEventWithAttributes(attrs, attrs);

        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_EXCLUDE, "foo,bar");

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertFalse(task.attributeExcludePatterns.get().isEmpty(), "Blacklist patterns must be compiled in onScheduled");
        assertEquals(2, task.attributeExcludePatterns.get().size(), "Must have exactly 2 compiled patterns");

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue()).getJsonObject("updatedAttributes");
        assertFalse(json.containsKey("foo"), "Blacklisted attribute 'foo' must be excluded");
        assertFalse(json.containsKey("bar"), "Blacklisted attribute 'bar' must be excluded");
        assertTrue(json.containsKey("baz"), "Non-blacklisted attribute 'baz' must be included");
    }

    @Test
    void testAttributeBlacklistRegexPatternExcludesMatchingAttributes() throws Exception {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("internal.token", "secret");
        attrs.put("internal.key", "private");
        attrs.put("filename", "data.csv");
        final ProvenanceEventRecord event = createEventWithAttributes(attrs, attrs);

        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_EXCLUDE, "internal\\..*");

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue()).getJsonObject("updatedAttributes");
        assertFalse(json.containsKey("internal.token"), "Blacklisted attribute must be excluded");
        assertFalse(json.containsKey("internal.key"), "Blacklisted attribute must be excluded");
        assertTrue(json.containsKey("filename"), "Non-blacklisted attribute must be included");
    }

    @Test
    void testNoAttributeFilterPreservesAllAttributes() throws Exception {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("foo", "foo-value");
        attrs.put("bar", "bar-value");
        final ProvenanceEventRecord event = createEventWithAttributes(attrs, attrs);

        final KafkaProvenanceReportingTask task = setup(event, defaultProperties(), 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertTrue(task.attributeIncludePatterns.get().isEmpty(), "Whitelist patterns must be empty when whitelist is not configured");
        assertTrue(task.attributeExcludePatterns.get().isEmpty(), "Blacklist patterns must be empty when blacklist is not configured");

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue()).getJsonObject("updatedAttributes");
        assertTrue(json.containsKey("foo"), "All attributes must be present when no filter is configured");
        assertTrue(json.containsKey("bar"), "All attributes must be present when no filter is configured");
    }

    @Test
    void testAttributeWhitelistFilterAppliesToBothPreviousAndUpdatedAttributes() throws Exception {
        final Map<String, String> prevAttrs = new HashMap<>();
        prevAttrs.put("keep", "prev-keep-val");
        prevAttrs.put("drop", "prev-drop-val");

        final Map<String, String> updatedAttrs = new HashMap<>();
        updatedAttrs.put("keep", "updated-keep-val");
        updatedAttrs.put("drop", "updated-drop-val");

        final ProvenanceEventRecord event = createEventWithAttributes(prevAttrs, updatedAttrs);

        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_INCLUDE, "keep");

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue());

        final JsonObject updated = json.getJsonObject("updatedAttributes");
        assertTrue(updated.containsKey("keep"), "Whitelisted key must be present in updatedAttributes");
        assertFalse(updated.containsKey("drop"), "Non-whitelisted key must be absent from updatedAttributes");

        final JsonObject previous = json.getJsonObject("previousAttributes");
        assertTrue(previous.containsKey("keep"), "Whitelisted key must be present in previousAttributes");
        assertFalse(previous.containsKey("drop"), "Non-whitelisted key must be absent from previousAttributes");
    }

    @Test
    void testInvalidRegexInAttributeFilterFailsPropertyValidation() {
        final Validator validator = KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_INCLUDE.getValidators().get(0);

        final ValidationResult valid = validator.validate("Attribute Whitelist", "foo,bar.*", null);
        assertTrue(valid.isValid(), "Valid patterns must pass the property validator");

        final ValidationResult invalid = validator.validate("Attribute Whitelist", "foo,[invalid", null);
        assertFalse(invalid.isValid(), "A malformed regex entry must fail the property validator");
        assertTrue(invalid.getExplanation().contains("[invalid"),
                "Validation explanation must name the offending entry");
    }

    @Test
    void testAttributeMaxLengthTrimsLongValues() throws Exception {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("short", "abc");
        attrs.put("long", "a".repeat(200));
        final ProvenanceEventRecord event = createEventWithAttributes(attrs, attrs);

        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.ATTRIBUTE_MAX_LENGTH, "50");

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(50, task.attributeMaxLength, "Effective max length must match configured value");

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue()).getJsonObject("updatedAttributes");
        assertEquals("abc", json.getString("short"), "Values within limit must not be modified");
        assertEquals(50, json.getString("long").length(), "Values exceeding limit must be trimmed to max length");
    }

    @Test
    void testAttributeMaxLengthAtGlobalMaxDisablesTrimming() throws Exception {
        final String longValue = "x".repeat(100);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("attr", longValue);
        final ProvenanceEventRecord event = createEventWithAttributes(attrs, attrs);

        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.ATTRIBUTE_MAX_LENGTH,
                String.valueOf(KafkaProvenanceReportingTask.NIFI_DEFAULT_MAX_ATTR_LENGTH));

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(-1, task.attributeMaxLength,
                "Effective max length must be -1 (disabled) when configured value equals the NiFi global limit");

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue()).getJsonObject("updatedAttributes");
        assertEquals(longValue, json.getString("attr"),
                "Attribute value must not be trimmed when max length is at the NiFi global limit");
    }

    @Test
    void testAttributeMaxLengthAboveGlobalMaxDisablesTrimming() throws Exception {
        final String longValue = "y".repeat(100);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("attr", longValue);
        final ProvenanceEventRecord event = createEventWithAttributes(attrs, attrs);

        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.ATTRIBUTE_MAX_LENGTH,
                String.valueOf(KafkaProvenanceReportingTask.NIFI_DEFAULT_MAX_ATTR_LENGTH + 1));

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(-1, task.attributeMaxLength,
                "Effective max length must be -1 (disabled) when configured value exceeds the NiFi global limit");

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue()).getJsonObject("updatedAttributes");
        assertEquals(longValue, json.getString("attr"),
                "Attribute value must not be trimmed when max length exceeds the NiFi global limit");
    }

    @Test
    void testAttributeMaxLengthBelowMinimumEnforcesFloorOf36() throws Exception {
        final String value = "a".repeat(40);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("attr", value);
        final ProvenanceEventRecord event = createEventWithAttributes(attrs, attrs);

        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.ATTRIBUTE_MAX_LENGTH, "10");

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(KafkaProvenanceReportingTask.ATTR_LENGTH_MIN, task.attributeMaxLength,
                "Effective max length must be enforced to " + KafkaProvenanceReportingTask.ATTR_LENGTH_MIN
                        + " when configured value is below the minimum");

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue()).getJsonObject("updatedAttributes");
        assertEquals(KafkaProvenanceReportingTask.ATTR_LENGTH_MIN, json.getString("attr").length(),
                "Attribute value must be trimmed to the enforced minimum of "
                        + KafkaProvenanceReportingTask.ATTR_LENGTH_MIN + " characters");
    }

    @Test
    void testAttributeMaxLengthCombinedWithWhitelistFilter() throws Exception {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("include", "a".repeat(200));
        attrs.put("exclude", "b".repeat(200));
        final ProvenanceEventRecord event = createEventWithAttributes(attrs, attrs);

        final Map<PropertyDescriptor, String> props = defaultProperties();
        props.put(KafkaProvenanceReportingTask.FILTER_ATTRIBUTES_INCLUDE, "include");
        props.put(KafkaProvenanceReportingTask.ATTRIBUTE_MAX_LENGTH, "50");

        final KafkaProvenanceReportingTask task = setup(event, props, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, sentRecords.size());
        final JsonObject json = parseJson(sentRecords.get(0).getValue()).getJsonObject("updatedAttributes");
        assertTrue(json.containsKey("include"), "Whitelisted attribute must be present");
        assertFalse(json.containsKey("exclude"), "Non-whitelisted attribute must be absent");
        assertEquals(50, json.getString("include").length(),
                "Whitelisted attribute value must be trimmed to max length");
    }
}
