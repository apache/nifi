/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.nifi.flowanalysis.rules;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

@Tags({"connection", "source", "load", "balance"})
@CapabilityDescription("Produces rule violation when a source processor does not have a Load-Balanced Connection (LBC) downstream"
        + " in all outgoing connections")
public class RequireLoadBalancedConnectionAfterSourceProcessor extends AbstractFlowAnalysisRule {


    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {

        Collection<GroupAnalysisResult> analysisResults = new HashSet<>();

        Map<String, VersionedProcessor> sourceProcessors = new HashMap<>();
        for (VersionedProcessor processor : processGroup.getProcessors()) {
                sourceProcessors.put(processor.getIdentifier(), processor);
        }
        // Remove all processors that are not source processors
        processGroup.getConnections().forEach(connection -> {
            if (connection.getDestination().getType() == ConnectableComponentType.PROCESSOR) {
                sourceProcessors.remove(connection.getDestination().getId());
            }
        });

        processGroup.getConnections().forEach(connection -> {
            if (connection.getSource().getId() != null) {
                VersionedProcessor sourceProcessor = sourceProcessors.get(connection.getSource().getId());
                if (sourceProcessor != null) {
                    // Check if the connection is a Load-Balanced Connection
                    if (Objects.equals(connection.getLoadBalanceStrategy(), "DO_NOT_LOAD_BALANCE")) {
                        // If not, we need to report this as a violation
                        analysisResults.add(
                                GroupAnalysisResult.forComponent(sourceProcessor,
                                        connection.getIdentifier() + "_" + "LoadBalancedConnectionRequired",
                                        "Source processors must configure their downstream connections to use a Load Balancing Strategy")
                                        .build());
                    }
                }
            }
        });

        return analysisResults;
    }

}
