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

package org.apache.nifi.components.validation;

import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.flow.FlowManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Gathers the components subject to the initial and periodic validation sweeps, so that
 * {@link TriggerValidationTask} and {@link ParallelTriggerValidationTask} validate exactly the same set of
 * components without duplicating the collection logic in both places.
 */
final class ValidatableComponents {

    private ValidatableComponents() {
    }

    static List<ComponentNode> getComponentNodes(final FlowManager flowManager) {
        final List<ComponentNode> nodes = new ArrayList<>();
        nodes.addAll(flowManager.getAllControllerServices());
        nodes.addAll(flowManager.getAllReportingTasks());
        nodes.addAll(flowManager.getAllFlowAnalysisRules());
        nodes.addAll(flowManager.getAllParameterProviders());
        nodes.addAll(flowManager.getRootGroup().findAllProcessors());
        nodes.addAll(flowManager.getAllFlowRegistryClients());
        return nodes;
    }

    static List<ConnectorNode> getConnectors(final FlowManager flowManager) {
        return new ArrayList<>(flowManager.getAllConnectors());
    }
}
