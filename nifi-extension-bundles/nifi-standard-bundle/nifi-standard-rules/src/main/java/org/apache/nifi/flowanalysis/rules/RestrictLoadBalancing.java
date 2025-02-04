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
package org.apache.nifi.flowanalysis.rules;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;
import org.apache.nifi.flowanalysis.rules.util.ConnectionViolation;
import org.apache.nifi.flowanalysis.rules.util.FlowAnalysisRuleUtils;
import org.apache.nifi.flowanalysis.rules.util.ViolationType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Tags({"connection", "load", "balance", "balancing", "strategy", "compress"})
@CapabilityDescription("This rule generates a violation when the Load Balance Strategy setting of a connection is set to a strategy which is not allowed. "
        + "This rule may be useful for preventing certain strategy types or load-balancing compression. It may be implemented to assist in locating "
        + "all connections which have load balancing enabled.")
public class RestrictLoadBalancing extends AbstractFlowAnalysisRule {

    static final String ALLOWED = "Allowed";
    static final String DISALLOWED = "Disallowed";
    static final AllowableValue ALLOWABLE_ALLOWED = new AllowableValue(ALLOWED, ALLOWED);
    static final AllowableValue ALLOWABLE_DISALLOWED = new AllowableValue(DISALLOWED, DISALLOWED);
    static final String CONFIGURE_STRATEGY_ERROR_MESSAGE = "is set to " + ALLOWED + " so at least one load balancing strategy property must be set to " + ALLOWED;

    public static final PropertyDescriptor LOAD_BALANCING_POLICY = new PropertyDescriptor.Builder()
            .name("Load Balancing Policy")
            .description("Determines whether any load balance strategy is allowed. In all cases, 'Do not load balance' is valid and will not cause a violation.")
            .required(true)
            .allowableValues(ALLOWABLE_ALLOWED, ALLOWABLE_DISALLOWED)
            .defaultValue(DISALLOWED)
            .build();

    public static final PropertyDescriptor ALLOW_PARTITION = new PropertyDescriptor.Builder()
            .name("Partition By Attribute Strategy")
            .description("Connections may be configured with the load balancing strategy of 'Partition by attribute'.")
            .required(false)
            .allowableValues(ALLOWABLE_ALLOWED, ALLOWABLE_DISALLOWED)
            .defaultValue(ALLOWED)
            .dependsOn(LOAD_BALANCING_POLICY, ALLOWED)
            .build();

    public static final PropertyDescriptor ALLOW_ROUND_ROBIN = new PropertyDescriptor.Builder()
            .name("Round Robin Strategy")
            .description("Connections may be configured with the load balancing strategy of 'Round robin'.")
            .required(false)
            .allowableValues(ALLOWABLE_ALLOWED, ALLOWABLE_DISALLOWED)
            .defaultValue(ALLOWED)
            .dependsOn(LOAD_BALANCING_POLICY, ALLOWED)
            .build();

    public static final PropertyDescriptor ALLOW_SINGLE_NODE = new PropertyDescriptor.Builder()
            .name("Single Node Strategy")
            .description("Connections may be configured with the load balancing strategy of 'Single node'.")
            .required(false)
            .allowableValues(ALLOWABLE_ALLOWED, ALLOWABLE_DISALLOWED)
            .defaultValue(ALLOWED)
            .dependsOn(LOAD_BALANCING_POLICY, ALLOWED)
            .build();

    public static final PropertyDescriptor ALLOW_ATTRIBUTE_COMPRESSION = new PropertyDescriptor.Builder()
            .name("Compress Attributes Only")
            .description("Connections which are configured with a load balancing strategy, may compress attributes.")
            .required(false)
            .allowableValues(ALLOWABLE_ALLOWED, ALLOWABLE_DISALLOWED)
            .defaultValue(ALLOWED)
            .dependsOn(LOAD_BALANCING_POLICY, ALLOWED)
            .build();

    public static final PropertyDescriptor ALLOW_CONTENT_COMPRESSION = new PropertyDescriptor.Builder()
            .name("Compress Attributes and Content")
            .description("Connections which are configured with a load balancing strategy, may compress both attributes and content.")
            .required(false)
            .allowableValues(ALLOWABLE_ALLOWED, ALLOWABLE_DISALLOWED)
            .defaultValue(ALLOWED)
            .dependsOn(LOAD_BALANCING_POLICY, ALLOWED)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            LOAD_BALANCING_POLICY,
            ALLOW_PARTITION,
            ALLOW_ROUND_ROBIN,
            ALLOW_SINGLE_NODE,
            ALLOW_ATTRIBUTE_COMPRESSION,
            ALLOW_CONTENT_COMPRESSION
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        // For this flow analysis rule to be valid, load balancing must be disallowed, or at least one load balancing strategy must be allowed
        if (validationContext.getProperty(LOAD_BALANCING_POLICY).getValue().equals(ALLOWED)
                && !isLoadBalancingStrategyAllowed(validationContext)) {
            results.add(new ValidationResult.Builder()
                    .subject(LOAD_BALANCING_POLICY.getName())
                    .valid(false)
                    .explanation(LOAD_BALANCING_POLICY.getName() + " " + CONFIGURE_STRATEGY_ERROR_MESSAGE)
                    .build());
        }

        return results;
    }

    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
        final Collection<ConnectionViolation> violations = new ArrayList<>();
        final boolean allowLoadBalancing = context.getProperty(LOAD_BALANCING_POLICY).getValue().equals(ALLOWED);
        final boolean allowPartition = context.getProperty(ALLOW_PARTITION).getValue().equals(ALLOWED);
        final boolean allowRoundRobin = context.getProperty(ALLOW_ROUND_ROBIN).getValue().equals(ALLOWED);
        final boolean allowSingleNode = context.getProperty(ALLOW_SINGLE_NODE).getValue().equals(ALLOWED);
        final boolean allowAttributesCompression = context.getProperty(ALLOW_ATTRIBUTE_COMPRESSION).getValue().equals(ALLOWED);
        final boolean allowAttributesAndContentCompression = context.getProperty(ALLOW_CONTENT_COMPRESSION).getValue().equals(ALLOWED);

        processGroup.getConnections().forEach(connection -> {
            final String strategy = connection.getLoadBalanceStrategy();

            if (isLoadBalancingEnabled(strategy) && !allowLoadBalancing) {
                violations.add(new ConnectionViolation(connection,
                        LoadBalanceViolationType.LOAD_BALANCE_DISALLOWED,
                        this.getClass().getSimpleName(),
                        connection.getLoadBalanceStrategy(),
                        context.getProperty(LOAD_BALANCING_POLICY).getValue()));
            }
            if (LoadBalanceStrategy.PARTITION_BY_ATTRIBUTE.name().equals(strategy) && !allowPartition) {
                violations.add(new ConnectionViolation(connection,
                        LoadBalanceViolationType.PARTITION_DISALLOWED,
                        this.getClass().getSimpleName(),
                        connection.getLoadBalanceStrategy(),
                        context.getProperty(ALLOW_PARTITION).getValue()));
            }
            if (LoadBalanceStrategy.ROUND_ROBIN.name().equals(strategy) && !allowRoundRobin) {
                    violations.add(new ConnectionViolation(connection,
                            LoadBalanceViolationType.ROUND_ROBIN_DISALLOWED,
                            this.getClass().getSimpleName(),
                            connection.getLoadBalanceStrategy(),
                            context.getProperty(ALLOW_ROUND_ROBIN).getValue()));
            }
            if (LoadBalanceStrategy.SINGLE_NODE.name().equals(strategy) && !allowSingleNode) {
                violations.add(new ConnectionViolation(connection,
                        LoadBalanceViolationType.SINGLE_NODE_DISALLOWED,
                        this.getClass().getSimpleName(),
                        connection.getLoadBalanceStrategy(),
                        context.getProperty(ALLOW_SINGLE_NODE).getValue()));
            }

            // Evaluate compression only if one of the load balance strategies is specified
            if (isLoadBalancingEnabled(strategy)) {
                String compression = connection.getLoadBalanceCompression();
                if (LoadBalanceCompression.COMPRESS_ATTRIBUTES_ONLY.name().equals(compression) && !allowAttributesCompression) {
                    violations.add(new ConnectionViolation(connection,
                            LoadBalanceViolationType.ATTRIBUTE_COMPRESSION_DISALLOWED,
                            this.getClass().getSimpleName(),
                            connection.getLoadBalanceCompression(),
                            context.getProperty(ALLOW_ATTRIBUTE_COMPRESSION).getValue()));
                }
                if (LoadBalanceCompression.COMPRESS_ATTRIBUTES_AND_CONTENT.name().equals(compression) && !allowAttributesAndContentCompression) {
                    violations.add(new ConnectionViolation(connection,
                            LoadBalanceViolationType.CONTENT_COMPRESSION_DISALLOWED,
                            this.getClass().getSimpleName(),
                            connection.getLoadBalanceCompression(),
                            context.getProperty(ALLOW_CONTENT_COMPRESSION).getValue()));
                }
            }
        });

        return FlowAnalysisRuleUtils.convertToGroupAnalysisResults(processGroup, violations);
    }

    private boolean isLoadBalancingStrategyAllowed(ValidationContext validationContext) {
        return validationContext.getProperty(ALLOW_PARTITION).getValue().equals(ALLOWED)
                || validationContext.getProperty(ALLOW_ROUND_ROBIN).getValue().equals(ALLOWED)
                || validationContext.getProperty(ALLOW_SINGLE_NODE).getValue().equals(ALLOWED);
    }

    private boolean isLoadBalancingEnabled(String strategy) {
        return LoadBalanceStrategy.PARTITION_BY_ATTRIBUTE.name().equals(strategy)
                || LoadBalanceStrategy.ROUND_ROBIN.name().equals(strategy)
                || LoadBalanceStrategy.SINGLE_NODE.name().equals(strategy);
    }

    private enum LoadBalanceViolationType implements ViolationType {

        LOAD_BALANCE_DISALLOWED("NoLoadBalanceNotAllowed", "Load Balance Strategy", "is disallowed because \"" + LOAD_BALANCING_POLICY.getName() + "\" is set to "),
        PARTITION_DISALLOWED("PartitionByAttributeNotAllowed", "Load Balance Strategy", "is disallowed because \"" + ALLOW_PARTITION.getName() + "\" is set to "),
        ROUND_ROBIN_DISALLOWED("RoundRobinNotAllowed", "Load Balance Strategy", "is disallowed because \"" + ALLOW_ROUND_ROBIN.getName() + "\" is set to "),
        SINGLE_NODE_DISALLOWED("SingleNodeNotAllowed", "Load Balance Strategy", "is disallowed because \"" + ALLOW_SINGLE_NODE.getName() + "\" is set to "),
        ATTRIBUTE_COMPRESSION_DISALLOWED("AttributeCompressionNotAllowed", "Load Balance Compression", "is disallowed because \"" + ALLOW_ATTRIBUTE_COMPRESSION.getName() + "\" is set to "),
        CONTENT_COMPRESSION_DISALLOWED("ContentCompressionNotAllowed", "Load Balance Compression", "is disallowed because \"" + ALLOW_CONTENT_COMPRESSION.getName() + "\" is set to ");

        private final String id;
        private final String configurationItem;
        private final String violationMessage;

        LoadBalanceViolationType(String id, String configurationItem, String violationMessage) {
            this.id = id;
            this.configurationItem = configurationItem;
            this.violationMessage = violationMessage;
        }

        @Override
        public String getId() {
            return this.id;
        }

        @Override
        public String getConfigurationItem() {
            return this.configurationItem;
        }

        @Override
        public String getViolationMessage() {
            return this.violationMessage;
        }
    }
}
