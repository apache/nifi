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
package org.apache.nifi.flowanalysis.rules.util;

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;
import org.apache.nifi.util.StringUtils;

import java.util.Map;

public class ConnectionViolation {
    private final VersionedConnection connection;
    private final ViolationType violationType;
    private final String rule;
    private final String configuredValue;
    private final String ruleLimit;

    public ConnectionViolation(VersionedConnection connection,
                               ViolationType violationType,
                               String rule,
                               String configuredValue,
                               String ruleLimit) {
        this.connection = connection;
        this.violationType = violationType;
        this.rule = rule;
        this.configuredValue = configuredValue;
        this.ruleLimit = ruleLimit;
    }

    /**
     * Convert this ConnectionViolation to a Flow Analysis Rule GroupAnalysisResult.
     *
     * @param components the components in a ProcessGroup that could be attached to this VersionedConnection.
     * @return a GroupAnalysisResult
     */
    public GroupAnalysisResult convertToGroupAnalysisResult(Map<String, VersionedComponent> components) {
        VersionedComponent source = components.get(connection.getSource().getId());
        VersionedComponent destination = components.get(connection.getDestination().getId());
        return GroupAnalysisResult.forComponent(
                        assignConnectionViolationToComponent(connection, source, destination),
                        connection.getIdentifier() + "_" + violationType.getId(),
                        getConnectionLocationMessage(connection, source, destination, rule)
                                + " " + getViolationReason())
                .build();
    }

    /*
     * Connection violations should be assigned to a connected Processor (either the source or destination)
     * because in case the rule is "enforced" we want the corresponding component to be invalid.
     * If neither connected component is a Processor (i.e. funnel, process group, port, etc.), then we cannot make
     * it invalid so put the violation on the connection itself.
     */
    private VersionedComponent assignConnectionViolationToComponent(
            final VersionedConnection connection, final VersionedComponent source, final VersionedComponent destination) {

        if (!(source instanceof VersionedProcessor) && !(destination instanceof VersionedProcessor)) {
            // for connections between two components that are not processors and cannot be invalid, set violation on connection
            return connection;
        } else if (source instanceof VersionedProcessor) {
            // set violation on source processor
            return source;
        } else {
            // set violation on destination processor
            return destination;
        }
    }

    /*
     * Format a message stating what configured value violates a rule.
     */
    private String getViolationReason() {
        return String.format("The connection is configured with a %s of %s which %s %s.",
                violationType.getConfigurationItem(), configuredValue, violationType.getViolationMessage(), ruleLimit);
    }

    /*
     * Format a rule violation message for a VersionedConnection with information about the source and destination VersionedComponent.
     */
    private String getConnectionLocationMessage(
            final VersionedConnection connection, final VersionedComponent source, final VersionedComponent destination, final String rule) {

        StringBuilder message = new StringBuilder();
        message.append("The connection ");
        if (StringUtils.isNotEmpty(connection.getName())) {
            // Output connection name if it has been named
            message.append(connection.getName()).append(' ');
        } else {
            // Output connection relationships by name
            String relationships = String.join(",", connection.getSelectedRelationships());
            if (StringUtils.isNotEmpty(relationships)) {
                message.append(relationships).append(' ');
            }
        }
        message.append("[").append(connection.getIdentifier()).append("]");
        if (source != null) {
            String sourceName =
                    (source instanceof VersionedFunnel) ? "funnel" : source.getName();
            message.append(" from source ").append(sourceName).append(" [").append(source.getIdentifier()).append("]");
        }
        if (destination != null) {
            String destinationName =
                    (destination instanceof VersionedFunnel) ? "funnel" : destination.getName();
            message.append(" to destination ").append(destinationName).append(" [").append(destination.getIdentifier()).append("]");
        }
        message.append(" violates the ").append(rule).append(" rule.");
        return message.toString();
    }
}
