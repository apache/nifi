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
package org.apache.nifi.controller.status.analytics;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.apache.nifi.groups.ProcessGroup;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public abstract class TestStatusAnalyticsEngine {

    static final long DEFAULT_PREDICT_INTERVAL_MILLIS = 3L * 60 * 1000;

    protected ComponentStatusRepository statusRepository;
    protected FlowManager flowManager;
    protected FlowFileEventRepository flowFileEventRepository;

    @Before
    public void setup() {

        statusRepository = Mockito.mock(ComponentStatusRepository.class);
        flowManager = Mockito.mock(FlowManager.class);
        ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);
        StatusHistory statusHistory = Mockito.mock(StatusHistory.class);
        StatusSnapshot statusSnapshot = Mockito.mock(StatusSnapshot.class);
        when(statusSnapshot.getMetricDescriptors()).thenReturn(Collections.emptySet());
        when(flowManager.getRootGroup()).thenReturn(processGroup);
        when(statusRepository.getConnectionStatusHistory(anyString(), any(), any(), anyInt())).thenReturn(statusHistory);
    }

    @Test
    public void testGetStatusAnalytics() {
        StatusAnalyticsEngine statusAnalyticsEngine = getStatusAnalyticsEngine(flowManager,flowFileEventRepository, statusRepository, DEFAULT_PREDICT_INTERVAL_MILLIS);
        StatusAnalytics statusAnalytics = statusAnalyticsEngine.getStatusAnalytics("1");
        assertNotNull(statusAnalytics);
    }

    public abstract StatusAnalyticsEngine getStatusAnalyticsEngine(FlowManager flowManager, FlowFileEventRepository flowFileEventRepository,
                                                                   ComponentStatusRepository componentStatusRepository, long predictIntervalMillis);

}
