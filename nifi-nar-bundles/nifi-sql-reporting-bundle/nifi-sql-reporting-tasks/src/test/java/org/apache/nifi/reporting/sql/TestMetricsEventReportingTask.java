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

import com.google.common.collect.Lists;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.analytics.ConnectionStatusPredictions;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.reporting.sql.util.QueryMetricsUtil;
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.MockPropertyContextActionHandler;
import org.apache.nifi.rules.PropertyContextActionHandler;
import org.apache.nifi.rules.engine.MockRulesEngineService;
import org.apache.nifi.rules.engine.RulesEngineService;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.util.db.JdbcProperties;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class TestMetricsEventReportingTask {
    private ReportingContext context;
    private MockMetricsEventReportingTask reportingTask;
    private MockPropertyContextActionHandler actionHandler;
    private MockRulesEngineService rulesEngineService;
    private ProcessGroupStatus status;

    @Before
    public void setup() {
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

        // create a group status with processing time
        ProcessGroupStatus groupStatus1 = new ProcessGroupStatus();
        groupStatus1.setProcessorStatus(processorStatuses);
        groupStatus1.setBytesRead(1234L);

        // Create a nested group status with a connection
        ProcessGroupStatus groupStatus2 = new ProcessGroupStatus();
        groupStatus2.setProcessorStatus(processorStatuses);
        groupStatus2.setBytesRead(12345L);
        ConnectionStatus nestedConnectionStatus = new ConnectionStatus();
        nestedConnectionStatus.setId("nested");
        nestedConnectionStatus.setQueuedCount(1001);
        Collection<ConnectionStatus> nestedConnectionStatuses = new ArrayList<>();
        nestedConnectionStatuses.add(nestedConnectionStatus);
        groupStatus2.setConnectionStatus(nestedConnectionStatuses);
        Collection<ProcessGroupStatus> nestedGroupStatuses = new ArrayList<>();
        nestedGroupStatuses.add(groupStatus2);
        groupStatus1.setProcessGroupStatus(nestedGroupStatuses);

        ProcessGroupStatus groupStatus3 = new ProcessGroupStatus();
        groupStatus3.setBytesRead(1L);
        ConnectionStatus nestedConnectionStatus2 = new ConnectionStatus();
        nestedConnectionStatus2.setId("nested2");
        nestedConnectionStatus2.setQueuedCount(3);
        Collection<ConnectionStatus> nestedConnectionStatuses2 = new ArrayList<>();
        nestedConnectionStatuses2.add(nestedConnectionStatus2);
        groupStatus3.setConnectionStatus(nestedConnectionStatuses2);
        Collection<ProcessGroupStatus> nestedGroupStatuses2 = new ArrayList<>();
        nestedGroupStatuses2.add(groupStatus3);

        Collection<ProcessGroupStatus> groupStatuses = new ArrayList<>();
        groupStatuses.add(groupStatus1);
        groupStatuses.add(groupStatus3);
        status.setProcessGroupStatus(groupStatuses);

    }

    @Test
    public void testConnectionStatusTable() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.QUERY, "select connectionId, predictedQueuedCount, predictedTimeToBytesBackpressureMillis from CONNECTION_STATUS_PREDICTIONS");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);
        List<Map<String,Object>> metricsList = actionHandler.getRows();
        List<Tuple<String, Action>> defaultLogActions = actionHandler.getDefaultActionsByType("LOG");
        List<Tuple<String, Action>> defaultAlertActions = actionHandler.getDefaultActionsByType("ALERT");
        List<PropertyContext> propertyContexts = actionHandler.getPropertyContexts();
        assertFalse(metricsList.isEmpty());
        assertEquals(2,defaultLogActions.size());
        assertEquals(2,defaultAlertActions.size());
        assertEquals(4,propertyContexts.size());

    }

    private MockMetricsEventReportingTask initTask(Map<PropertyDescriptor, String> customProperties) throws InitializationException, IOException {

        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        final BulletinRepository bulletinRepository = Mockito.mock(BulletinRepository.class);
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
        Mockito.when(context.getStateManager()).thenReturn(new MockStateManager(reportingTask));
        Mockito.when(context.getBulletinRepository()).thenReturn(bulletinRepository);
        Mockito.when(context.createBulletin(anyString(),any(Severity.class), anyString())).thenReturn(null);

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

        Action action1 = new Action();
        action1.setType("LOG");
        Action action2 = new Action();
        action2.setType("ALERT");

        final PropertyValue resValue = Mockito.mock(StandardPropertyValue.class);
        rulesEngineService = new MockRulesEngineService(Lists.newArrayList(action1,action2));
        Mockito.when(resValue.asControllerService(RulesEngineService.class)).thenReturn(rulesEngineService);

        ConfigurationContext configContext = Mockito.mock(ConfigurationContext.class);
        Mockito.when(configContext.getProperty(QueryMetricsUtil.RULES_ENGINE)).thenReturn(resValue);
        Mockito.when(configContext.getProperty(QueryMetricsUtil.ACTION_HANDLER)).thenReturn(pValue);
        Mockito.when(configContext.getProperty(JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION)).thenReturn(new MockPropertyValue("10"));
        Mockito.when(configContext.getProperty(JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE)).thenReturn(new MockPropertyValue("0"));
        reportingTask.setup(configContext);

        return reportingTask;
    }

    private static final class MockMetricsEventReportingTask extends MetricsEventReportingTask {

    }
}
