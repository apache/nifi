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
package org.apache.nifi.reporting.sql;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.analytics.ConnectionStatusPredictions;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinFactory;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.sql.util.QueryMetricsUtil;
import org.apache.nifi.reporting.sql.util.TrackedQueryTime;
import org.apache.nifi.rules.MockPropertyContextActionHandler;
import org.apache.nifi.rules.PropertyContextActionHandler;
import org.apache.nifi.rules.engine.MockRulesEngineService;
import org.apache.nifi.rules.engine.RulesEngineService;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockBulletinRepository;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.db.JdbcProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class TestMetricsEventReportingTask {

    private ReportingContext context;
    private MockMetricsEventReportingTask reportingTask;
    private MockPropertyContextActionHandler actionHandler;
    private ProcessGroupStatus status;
    private MockQueryBulletinRepository mockBulletinRepository;
    private MockProvenanceRepository mockProvenanceRepository;
    private AtomicLong currentTime;
    private MockStateManager mockStateManager;

    @BeforeEach
    public void setup() {
        currentTime = new AtomicLong();
        status = new ProcessGroupStatus();
        actionHandler = new MockPropertyContextActionHandler();
        status.setId("1234");
        status.setFlowFilesReceived(5);
        status.setBytesReceived(10000);
        status.setFlowFilesSent(10);
        status.setBytesRead(20000L);
        status.setBytesSent(20000);
        status.setQueuedCount(100);
        status.setQueuedContentSize(1024L);
        status.setBytesWritten(80000L);
        status.setActiveThreadCount(5);

        // create a processor status with processing time
        ProcessorStatus procStatus = new ProcessorStatus();
        procStatus.setId("proc");
        procStatus.setProcessingNanos(123456789);

        Collection<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        status.setProcessorStatus(processorStatuses);

        ConnectionStatusPredictions connectionStatusPredictions = new ConnectionStatusPredictions();
        connectionStatusPredictions.setPredictedTimeToCountBackpressureMillis(1000);
        connectionStatusPredictions.setPredictedTimeToBytesBackpressureMillis(1000);
        connectionStatusPredictions.setNextPredictedQueuedCount(1000000000);
        connectionStatusPredictions.setNextPredictedQueuedBytes(1000000000000000L);

        ConnectionStatus root1ConnectionStatus = new ConnectionStatus();
        root1ConnectionStatus.setId("root1");
        root1ConnectionStatus.setQueuedCount(1000);
        root1ConnectionStatus.setPredictions(connectionStatusPredictions);

        ConnectionStatus root2ConnectionStatus = new ConnectionStatus();
        root2ConnectionStatus.setId("root2");
        root2ConnectionStatus.setQueuedCount(500);
        root2ConnectionStatus.setPredictions(connectionStatusPredictions);

        Collection<ConnectionStatus> rootConnectionStatuses = new ArrayList<>();
        rootConnectionStatuses.add(root1ConnectionStatus);
        rootConnectionStatuses.add(root2ConnectionStatus);
        status.setConnectionStatus(rootConnectionStatuses);
    }

    @Test
    void testConnectionStatusTable() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.QUERY, "select connectionId, predictedQueuedCount, predictedTimeToBytesBackpressureMillis from CONNECTION_STATUS_PREDICTIONS");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);
        List<PropertyContext> propertyContexts = actionHandler.getPropertyContexts();
        assertEquals(2, actionHandler.getRows().size());
        assertEquals(2, propertyContexts.size());
    }

    @Test
    void testUniqueBulletinQueryIsInTimeWindow() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.QUERY, "select bulletinCategory from BULLETINS where bulletinTimestamp > $bulletinStartTime and bulletinTimestamp <= $bulletinEndTime");
        reportingTask = initTask(properties);
        currentTime.set(Instant.now().toEpochMilli());
        reportingTask.onTrigger(context);
        assertEquals(1, actionHandler.getRows().size());

        actionHandler.reset();
        final Bulletin bulletin = BulletinFactory.createBulletin(ComponentType.CONTROLLER_SERVICE.name().toLowerCase(), "WARN", "test bulletin 2", "testFlowFileUuid");
        mockBulletinRepository.addBulletin(bulletin);
        currentTime.set(bulletin.getTimestamp().getTime());
        reportingTask.onTrigger(context);
        assertEquals(1, actionHandler.getRows().size());
    }

    @Test
    void testUniqueBulletinQueryIsOutOfTimeWindow() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.QUERY, "select bulletinCategory from BULLETINS where bulletinTimestamp > $bulletinStartTime and bulletinTimestamp <= $bulletinEndTime");
        reportingTask = initTask(properties);
        currentTime.set(Instant.now().toEpochMilli());
        reportingTask.onTrigger(context);
        assertEquals(1, actionHandler.getRows().size());

        actionHandler.reset();
        final Bulletin bulletin = BulletinFactory.createBulletin(ComponentType.CONTROLLER_SERVICE.name().toLowerCase(), "WARN", "test bulletin 2", "testFlowFileUuid");
        mockBulletinRepository.addBulletin(bulletin);
        currentTime.set(bulletin.getTimestamp().getTime() - 1);
        reportingTask.onTrigger(context);
        assertEquals(0, actionHandler.getRows().size());
    }

    @Test
    void testUniqueProvenanceQueryIsInTimeWindow() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.QUERY, "select componentId from PROVENANCE where timestampMillis > $provenanceStartTime and timestampMillis <= $provenanceEndTime");
        reportingTask = initTask(properties);
        currentTime.set(Instant.now().toEpochMilli());
        reportingTask.onTrigger(context);
        assertEquals(1, actionHandler.getRows().size());

        actionHandler.reset();

        MockFlowFile mockFlowFile = new MockFlowFile(2L);
        ProvenanceEventRecord prov2 = mockProvenanceRepository.eventBuilder()
                .setEventType(ProvenanceEventType.CREATE)
                .fromFlowFile(mockFlowFile)
                .setComponentId("2")
                .setComponentType("ReportingTask")
                .setFlowFileUUID("I am FlowFile 2")
                .setEventTime(Instant.now().toEpochMilli())
                .setEventDuration(100)
                .setTransitUri("test://")
                .setSourceSystemFlowFileIdentifier("I am FlowFile 2")
                .setAlternateIdentifierUri("remote://test")
                .build();
        mockProvenanceRepository.registerEvent(prov2);

        currentTime.set(prov2.getEventTime());
        reportingTask.onTrigger(context);

        assertEquals(1, actionHandler.getRows().size());
    }

    @Test
    void testUniqueProvenanceQueryIsOutOfTimeWindow() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.QUERY, "select componentId from PROVENANCE where timestampMillis > $provenanceStartTime and timestampMillis <= $provenanceEndTime");
        reportingTask = initTask(properties);
        currentTime.set(Instant.now().toEpochMilli());
        reportingTask.onTrigger(context);
        assertEquals(1, actionHandler.getRows().size());

        actionHandler.reset();

        MockFlowFile mockFlowFile = new MockFlowFile(2L);
        ProvenanceEventRecord prov2 = mockProvenanceRepository.eventBuilder()
                .setEventType(ProvenanceEventType.CREATE)
                .fromFlowFile(mockFlowFile)
                .setComponentId("2")
                .setComponentType("ReportingTask")
                .setFlowFileUUID("I am FlowFile 2")
                .setEventTime(Instant.now().toEpochMilli())
                .setEventDuration(100)
                .setTransitUri("test://")
                .setSourceSystemFlowFileIdentifier("I am FlowFile 2")
                .setAlternateIdentifierUri("remote://test")
                .build();
        mockProvenanceRepository.registerEvent(prov2);

        currentTime.set(prov2.getEventTime() - 1);
        reportingTask.onTrigger(context);

        assertEquals(0, actionHandler.getRows().size());
    }

    @Test
    void testTimeWindowFromStateMap() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from BULLETINS, PROVENANCE where " +
                "bulletinTimestamp > $bulletinStartTime and bulletinTimestamp <= $bulletinEndTime " +
                "and timestampMillis > $provenanceStartTime and timestampMillis <= $provenanceEndTime");
        reportingTask = initTask(properties);

        long testBulletinStartTime = 1609538145L;
        long testProvenanceStartTime = 1641074145L;
        final Map<String, String> stateMap = new HashMap<>();
        stateMap.put(TrackedQueryTime.BULLETIN_START_TIME.name(), String.valueOf(testBulletinStartTime));
        stateMap.put(TrackedQueryTime.PROVENANCE_START_TIME.name(), String.valueOf(testProvenanceStartTime));
        mockStateManager.setState(stateMap, Scope.LOCAL);

        final long bulletinStartTime = Long.parseLong(context.getStateManager().getState(Scope.LOCAL).get(TrackedQueryTime.BULLETIN_START_TIME.name()));
        final long provenanceStartTime = Long.parseLong(context.getStateManager().getState(Scope.LOCAL).get(TrackedQueryTime.PROVENANCE_START_TIME.name()));

        assertEquals(testBulletinStartTime, bulletinStartTime);
        assertEquals(testProvenanceStartTime, provenanceStartTime);

        final long currentTime = Instant.now().toEpochMilli();
        this.currentTime.set(currentTime);

        reportingTask.onTrigger(context);

        final long updatedBulletinStartTime = Long.parseLong(context.getStateManager().getState(Scope.LOCAL).get(TrackedQueryTime.BULLETIN_START_TIME.name()));
        final long updatedProvenanceStartTime = Long.parseLong(context.getStateManager().getState(Scope.LOCAL).get(TrackedQueryTime.PROVENANCE_START_TIME.name()));

        assertEquals(currentTime, updatedBulletinStartTime);
        assertEquals(currentTime, updatedProvenanceStartTime);
    }

    private MockMetricsEventReportingTask initTask(Map<PropertyDescriptor, String> customProperties) throws InitializationException {
        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        reportingTask = new MockMetricsEventReportingTask();
        final ReportingInitializationContext initContext = Mockito.mock(ReportingInitializationContext.class);
        Mockito.when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(initContext.getLogger()).thenReturn(logger);
        reportingTask.initialize(initContext);
        Map<PropertyDescriptor, String> properties = new HashMap<>();

        for (final PropertyDescriptor descriptor : reportingTask.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.putAll(customProperties);

        context = Mockito.mock(ReportingContext.class);
        Mockito.when(context.isAnalyticsEnabled()).thenReturn(true);
        mockStateManager = new MockStateManager(reportingTask);
        Mockito.when(context.getStateManager()).thenReturn(mockStateManager);

        Mockito.doAnswer((Answer<PropertyValue>) invocation -> {
            final PropertyDescriptor descriptor = invocation.getArgument(0, PropertyDescriptor.class);
            return new MockPropertyValue(properties.get(descriptor));
        }).when(context).getProperty(Mockito.any(PropertyDescriptor.class));

        final EventAccess eventAccess = Mockito.mock(EventAccess.class);
        Mockito.when(context.getEventAccess()).thenReturn(eventAccess);
        Mockito.when(eventAccess.getControllerStatus()).thenReturn(status);

        final PropertyValue pValue = Mockito.mock(StandardPropertyValue.class);
        actionHandler = new MockPropertyContextActionHandler();
        Mockito.when(pValue.asControllerService(PropertyContextActionHandler.class)).thenReturn(actionHandler);

        final PropertyValue resValue = Mockito.mock(StandardPropertyValue.class);
        MockRulesEngineService rulesEngineService = new MockRulesEngineService();
        Mockito.when(resValue.asControllerService(RulesEngineService.class)).thenReturn(rulesEngineService);

        ConfigurationContext configContext = Mockito.mock(ConfigurationContext.class);
        Mockito.when(configContext.getProperty(QueryMetricsUtil.RULES_ENGINE)).thenReturn(resValue);
        Mockito.when(configContext.getProperty(QueryMetricsUtil.ACTION_HANDLER)).thenReturn(pValue);
        Mockito.when(configContext.getProperty(JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION)).thenReturn(new MockPropertyValue("10"));
        Mockito.when(configContext.getProperty(JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE)).thenReturn(new MockPropertyValue("0"));
        reportingTask.setup(configContext);

        setupMockProvenanceRepository(eventAccess);
        setupMockBulletinRepository();

        return reportingTask;
    }

    private final class MockMetricsEventReportingTask extends MetricsEventReportingTask {
        @Override
        public long getCurrentTime() {
            return currentTime.get();
        }
    }

    private void setupMockBulletinRepository() {
        mockBulletinRepository = new MockQueryBulletinRepository();
        mockBulletinRepository.addBulletin(BulletinFactory.createBulletin(ComponentType.PROCESSOR.name().toLowerCase(), "WARN", "test bulletin 1", "testFlowFileUuid"));

        Mockito.when(context.getBulletinRepository()).thenReturn(mockBulletinRepository);
    }

    private void setupMockProvenanceRepository(final EventAccess eventAccess) {

        mockProvenanceRepository = new MockProvenanceRepository();
        long currentTimeMillis = System.currentTimeMillis();
        Map<String, String> previousAttributes = new HashMap<>();
        previousAttributes.put("mime.type", "application/json");
        previousAttributes.put("test.value", "A");
        Map<String, String> updatedAttributes = new HashMap<>(previousAttributes);
        updatedAttributes.put("test.value", "B");

        // Generate provenance events and put them in a repository
        Processor processor = mock(Processor.class);
        SharedSessionState sharedState = new SharedSessionState(processor, new AtomicLong(0));
        MockProcessSession processSession = new MockProcessSession(sharedState, processor);
        MockFlowFile mockFlowFile = processSession.createFlowFile("Test content".getBytes());

        ProvenanceEventRecord prov1 = mockProvenanceRepository.eventBuilder()
                .setEventType(ProvenanceEventType.CREATE)
                .fromFlowFile(mockFlowFile)
                .setComponentId("1")
                .setComponentType("ReportingTask")
                .setFlowFileUUID("I am FlowFile 1")
                .setEventTime(currentTimeMillis)
                .setEventDuration(100)
                .setTransitUri("test://")
                .setSourceSystemFlowFileIdentifier("I am FlowFile 1")
                .setAlternateIdentifierUri("remote://test")
                .setAttributes(previousAttributes, updatedAttributes)
                .build();

        mockProvenanceRepository.registerEvent(prov1);

        Mockito.when(eventAccess.getProvenanceRepository()).thenReturn(mockProvenanceRepository);
    }

    private static class MockQueryBulletinRepository extends MockBulletinRepository {
        Map<String, List<Bulletin>> bulletins = new HashMap<>();

        @Override
        public void addBulletin(Bulletin bulletin) {
            bulletins.computeIfAbsent(bulletin.getCategory(), key -> new ArrayList<>())
                    .add(bulletin);
        }

        @Override
        public List<Bulletin> findBulletins(BulletinQuery bulletinQuery) {
            return new ArrayList<>(
                    Optional.ofNullable(bulletins.get(bulletinQuery.getSourceType().name().toLowerCase()))
                            .orElse(Collections.emptyList())
            );
        }

        @Override
        public List<Bulletin> findBulletinsForController() {
            return Optional.ofNullable(bulletins.get("controller"))
                    .orElse(Collections.emptyList());
        }
    }
}