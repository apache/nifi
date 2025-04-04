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
package org.apache.nifi.action;

import org.apache.nifi.admin.service.AuditService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@ExtendWith(MockitoExtension.class)
class ReportableAuditServiceTest {

    @Mock
    private AuditService auditService;
    private InMemoryFlowActionReporter flowActionReporter;
    private ReportableAuditService reportableAuditService;

    @BeforeEach
    void setUp() {
        flowActionReporter = new InMemoryFlowActionReporter();
        reportableAuditService = new ReportableAuditService(auditService, flowActionReporter, new HeadlessActionConverter());
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void shouldDecorateAuditService() {
        assertInstanceOf(AuditService.class, reportableAuditService);
    }

    @Test
    void shouldReportActions() {
        final List<Action> actions = new ArrayList<>();
        actions.add(new FlowChangeAction());

        reportableAuditService.addActions(actions);

        assertEquals(1, flowActionReporter.getReportedActions().size());
    }

    private static class InMemoryFlowActionReporter implements FlowActionReporter {
        List<FlowAction> reportedActions = new ArrayList<>();
        public void reportFlowActions(Collection<FlowAction> actions) {
            reportedActions.addAll(actions);
        }

        List<FlowAction> getReportedActions() {
            return reportedActions;
        }

    }

}