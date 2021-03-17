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
package org.apache.nifi.flowanalysis;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisUtil;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.integration.flowanalysis.DelegateFlowAnalysisRule;
import org.apache.nifi.integration.processors.NopProcessor;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.junit.jupiter.api.BeforeEach;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

public abstract class AbstractFlowAnalysisIT extends FrameworkIntegrationTest {
    protected NiFiRegistryFlowMapper mapper;

    @BeforeEach()
    public void setUpAbstractFlowAnalysisIT() throws Exception {
        mapper = FlowAnalysisUtil.createMapper(getFlowController().getExtensionManager());
    }

    protected ProcessGroup createProcessGroup(ProcessGroup parent) {
        String id = UUID.randomUUID().toString();

        ProcessGroup processGroup = getFlowController().getFlowManager().createProcessGroup(id);

        processGroup.setName(id);
        processGroup.setParent(parent);

        parent.addProcessGroup(processGroup);

        return processGroup;
    }

    protected ProcessorNode createProcessorNode(ProcessGroup processGroup) {
        ProcessorNode processorNode = getFlowController().getFlowManager().createProcessor(
            NopProcessor.class.getSimpleName(),
            UUID.randomUUID().toString(),
            SystemBundle.SYSTEM_BUNDLE_COORDINATE,
            Collections.emptySet(),
            true,
            true,
            null
        );

        processGroup.addProcessor(processorNode);

        return processorNode;
    }

    protected Connection createConnection(
            ProcessGroup processGroup,
            String name,
            Connectable source,
            Connectable destination,
            Collection<String> relationshipNames
    ) {
        Connection connection = getFlowController().getFlowManager().createConnection(
            UUID.randomUUID().toString(),
            name,
            source,
            destination,
            relationshipNames
        );

        processGroup.addConnection(connection);

        return connection;
    }

    protected VersionedProcessGroup mapProcessGroup(ProcessGroup processGroup) {
        return mapper.mapProcessGroup(
            processGroup,
            getFlowController().getControllerServiceProvider(),
            getFlowController().getFlowManager(),
            true
        );
    }

    protected FlowAnalysisRuleNode createAndEnableFlowAnalysisRuleNode(FlowAnalysisRule flowAnalysisRule) {
        FlowAnalysisRuleNode flowAnalysisRuleNode = createFlowAnalysisRuleNode(flowAnalysisRule);

        flowAnalysisRuleNode.enable();

        return flowAnalysisRuleNode;
    }

    protected FlowAnalysisRuleNode createFlowAnalysisRuleNode(FlowAnalysisRule flowAnalysisRule) {
        final FlowAnalysisRuleNode flowAnalysisRuleNode = getFlowController().getFlowManager().createFlowAnalysisRule(
            DelegateFlowAnalysisRule.class.getName(),
            UUID.randomUUID().toString(),
            SystemBundle.SYSTEM_BUNDLE_COORDINATE,
            Collections.emptySet(),
            true,
            true,
            null
        );

        ((DelegateFlowAnalysisRule) flowAnalysisRuleNode.getFlowAnalysisRule()).setDelegate(flowAnalysisRule);

        return flowAnalysisRuleNode;
    }
}
