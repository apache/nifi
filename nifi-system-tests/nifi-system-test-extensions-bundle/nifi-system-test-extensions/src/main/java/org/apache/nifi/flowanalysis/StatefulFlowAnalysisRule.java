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

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Stateful(scopes = Scope.LOCAL, description = "Stores the timestamp of the last initialization")
public class StatefulFlowAnalysisRule extends AbstractFlowAnalysisRule {
    private final String LAST_ANALYIZE_COMPONENT_TIMESTAMP = "last.analyze.component.timestamp";
    private final String LAST_ANALYIZE_PROCESS_GROUP_TIMESTAMP = "last.analyze.component.timestamp";

    @Override
    public Collection<ComponentAnalysisResult> analyzeComponent(VersionedComponent component, FlowAnalysisRuleContext context) {
        final String lastAnalyizeTimestampKey = LAST_ANALYIZE_COMPONENT_TIMESTAMP;

        updateState(context, lastAnalyizeTimestampKey);

        return super.analyzeComponent(component, context);
    }

    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
        updateState(context, LAST_ANALYIZE_PROCESS_GROUP_TIMESTAMP);

        return super.analyzeProcessGroup(processGroup, context);
    }

    private void updateState(FlowAnalysisRuleContext context, String lastAnalyizeTimestampKey) {
        final StateManager stateManager = context.getStateManager();

        final Map<String, String> state = new HashMap<>();
        state.put(lastAnalyizeTimestampKey, System.currentTimeMillis() + "");

        try {
            stateManager.setState(state, Scope.LOCAL);
        } catch (IOException e) {
            throw new ProcessException("Couldn't set state");
        }
    }
}
