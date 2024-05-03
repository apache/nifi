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
package org.apache.nifi.registry.flow;

import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.flowanalysis.FlowAnalyzer;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.EnforcementPolicy;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.validation.RuleViolation;
import org.apache.nifi.validation.RuleViolationsManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FlowAnalyzingRegistryClientNodeTest {
    private final static String INSTANCE_IDENTIFIER = UUID.randomUUID().toString();
    private final static String COMMENT_TEXT = "comment";
    private final static String EXPECTED_VERSION = "3";

    @Mock
    FlowRegistryClientNode node;

    @Mock
    ControllerServiceProvider serviceProvider;

    @Mock
    FlowAnalyzer flowAnalyzer;

    @Mock
    RuleViolationsManager ruleViolationsManager;

    @Mock
    FlowManager flowManager;

    @Mock
    NiFiRegistryFlowMapper flowMapper;

    @Mock
    InstantiatedVersionedProcessGroup nonVersionedProcessGroup;

    @Mock
    ProcessGroup processGroup;

    @Mock
    VersionedProcessGroup versionedProcessGroup;

    @Mock
    RuleViolation ruleViolation1;

    @Mock
    RuleViolation ruleViolation2;

    @Mock
    RuleViolation ruleViolation3;

    private final FlowRegistryClientUserContext context = new StandardFlowRegistryClientUserContext();
    private final RegisteredFlow flow = new RegisteredFlow();

    @BeforeEach
    public void setUp() {
        Mockito.when(versionedProcessGroup.getInstanceIdentifier()).thenReturn(INSTANCE_IDENTIFIER);
        Mockito.when(flowManager.getGroup(Mockito.anyString())).thenReturn(processGroup);
        Mockito.when(flowMapper.mapNonVersionedProcessGroup(Mockito.same(processGroup), Mockito.same(serviceProvider))).thenReturn(nonVersionedProcessGroup);
        Mockito.when(ruleViolation1.getEnforcementPolicy()).thenReturn(EnforcementPolicy.ENFORCE);
        Mockito.when(ruleViolation2.getEnforcementPolicy()).thenReturn(EnforcementPolicy.ENFORCE);
        Mockito.when(ruleViolation3.getEnforcementPolicy()).thenReturn(EnforcementPolicy.WARN);
    }

    @Test
    public void allowFlowRegistrationWhenNoEnforcingViolationFound() throws IOException, FlowRegistryException {
        Mockito.when(ruleViolationsManager.getRuleViolationsForGroup(Mockito.anyString())).thenReturn(Collections.emptyList());
        final FlowAnalyzingRegistryClientNode testSubject = new FlowAnalyzingRegistryClientNode(node, serviceProvider, flowAnalyzer, ruleViolationsManager, flowManager, flowMapper);

        testSubject.registerFlowSnapshot(context, flow, versionedProcessGroup, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                COMMENT_TEXT, EXPECTED_VERSION, RegisterAction.COMMIT);

        Mockito
            .verify(node, Mockito.only())
            .registerFlowSnapshot(context, flow, versionedProcessGroup, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                    COMMENT_TEXT, EXPECTED_VERSION, RegisterAction.COMMIT);
    }

    @Test
    public void allowFlowRegistrationWhenWarningViolationFound() throws IOException, FlowRegistryException {
        Mockito.when(ruleViolationsManager.getRuleViolationsForGroup(Mockito.anyString())).thenReturn(Collections.singletonList(ruleViolation3));
        final FlowAnalyzingRegistryClientNode testSubject = new FlowAnalyzingRegistryClientNode(node, serviceProvider, flowAnalyzer, ruleViolationsManager, flowManager, flowMapper);

        testSubject.registerFlowSnapshot(context, flow, versionedProcessGroup, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                COMMENT_TEXT, EXPECTED_VERSION, RegisterAction.COMMIT);

        Mockito
            .verify(node, Mockito.only())
            .registerFlowSnapshot(context, flow, versionedProcessGroup, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                    COMMENT_TEXT, EXPECTED_VERSION, RegisterAction.COMMIT);
    }

    @Test
    public void preventFlowRegistrationWhenEnforcingViolationFound() throws IOException, FlowRegistryException {
        Mockito.when(ruleViolationsManager.getRuleViolationsForGroup(Mockito.anyString())).thenReturn(Arrays.asList(ruleViolation1, ruleViolation2));
        final FlowAnalyzingRegistryClientNode testSubject = new FlowAnalyzingRegistryClientNode(node, serviceProvider, flowAnalyzer, ruleViolationsManager, flowManager, flowMapper);

        Assertions.assertThrows(
            FlowRegistryPreCommitException.class,
            () -> testSubject.registerFlowSnapshot(context, flow, versionedProcessGroup, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                    COMMENT_TEXT, EXPECTED_VERSION, RegisterAction.COMMIT)
        );

        Mockito
            .verify(node, Mockito.never())
            .registerFlowSnapshot(context, flow, versionedProcessGroup, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                    COMMENT_TEXT, EXPECTED_VERSION, RegisterAction.COMMIT);
    }
}