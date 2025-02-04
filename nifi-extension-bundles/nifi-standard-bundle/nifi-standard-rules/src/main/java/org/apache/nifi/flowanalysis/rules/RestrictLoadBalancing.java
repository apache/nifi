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
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Tags({"connection", "load", "balance", "strategy", "compression"})
@CapabilityDescription("This rule generates a violation when the Load Balance Strategy setting of a connection is set to a strategy which is not allowed. "
        + "This rule may be useful for preventing certain strategy types or load-balancing compression. It may be implemented to assist in locating "
        + "all connections which have load balancing enabled.")
public class RestrictLoadBalancing extends AbstractFlowAnalysisRule {

    static final String CONFIGURE_STRATEGY_ERROR_MESSAGE = "at least one load balance strategy property must be set to 'true'";
    static final String COMPRESSION_CONFIGURATION_ERROR_MESSAGE =
            "there is no compression configuration for content only without including attributes. If content compression is allowed, attribute compression must also be allowed.";

    public static final PropertyDescriptor ALLOW_DO_NOT_LOAD_BALANCE = new PropertyDescriptor.Builder()
            .name("Allow 'Do not load balance' Strategy")
            .description("If set to true, connections may be configured with the load balancing strategy of 'Do not load balance'. This is the default "
                    + "setting for new connections.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor ALLOW_PARTITION = new PropertyDescriptor.Builder()
            .name("Allow 'Partition by attribute' Strategy")
            .description("If set to true, connections may be configured with the load balancing strategy of 'Partition by attribute'.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor ALLOW_ROUND_ROBIN = new PropertyDescriptor.Builder()
            .name("Allow 'Round robin' Strategy")
            .description("If set to true, connections may be configured with the load balancing strategy of 'Round robin'.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor ALLOW_SINGLE_NODE = new PropertyDescriptor.Builder()
            .name("Allow 'Single node' Strategy")
            .description("If set to true, connections may be configured with the load balancing strategy of 'Single node'.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor ALLOW_ATTRIBUTE_COMPRESSION = new PropertyDescriptor.Builder()
            .name("Allow Attribute Compression")
            .description("If set to true, connections which are configured to load balance may compress attributes. "
                    + "This property is ignored if the only allowed Load Balance Strategy is 'Do not load balance'.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor ALLOW_CONTENT_COMPRESSION = new PropertyDescriptor.Builder()
            .name("Allow Content Compression")
            .description("If set to true, connections which are configured to load balance may compress attributes and content. "
                    + "This property is ignored if the only allowed Load Balance Strategy is 'Do not load balance'. "
                    + "When set to true, 'Allow Attribute Compression' must also be set to true")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ALLOW_DO_NOT_LOAD_BALANCE,
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

        // For this flow analysis rule to be valid, at least one load balancing strategy must be allowed
        if (!validationContext.getProperty(ALLOW_DO_NOT_LOAD_BALANCE).asBoolean()
                && !isLoadBalancingAllowed(validationContext)) {
            results.add(new ValidationResult.Builder()
                    .subject("Load Balance Strategy Properties")
                    .valid(false)
                    .explanation(CONFIGURE_STRATEGY_ERROR_MESSAGE)
                    .build());
        }

        // The only forms of compression are "attributes only" and "both attributes and content".
        // Therefore, if content compression is allowed, attribute compression must also be allowed.
        // It is also valid to have only attribute compression or no compression at all allowed.
        // In addition, this is only relevant if a load balance strategy is allowed, i.e. if the only allowed strategy is "Do not load balance",
        // then any combination of compression is allowed because it will be ignored.
        if (isLoadBalancingAllowed(validationContext)
                && validationContext.getProperty(ALLOW_CONTENT_COMPRESSION).asBoolean()
                && !validationContext.getProperty(ALLOW_ATTRIBUTE_COMPRESSION).asBoolean()) {
            results.add(new ValidationResult.Builder()
                    .subject(ALLOW_ATTRIBUTE_COMPRESSION.getName())
                    .valid(false)
                    .explanation(COMPRESSION_CONFIGURATION_ERROR_MESSAGE)
                    .build());
        }

        return results;
    }

    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
        final Collection<ConnectionViolation> violations = new ArrayList<>();
        final boolean allowNone = context.getProperty(ALLOW_DO_NOT_LOAD_BALANCE).asBoolean();
        final boolean allowPartition = context.getProperty(ALLOW_PARTITION).asBoolean();
        final boolean allowRoundRobin = context.getProperty(ALLOW_ROUND_ROBIN).asBoolean();
        final boolean allowSingleNode = context.getProperty(ALLOW_SINGLE_NODE).asBoolean();
        final boolean allowAttributeCompression = context.getProperty(ALLOW_ATTRIBUTE_COMPRESSION).asBoolean();
        final boolean allowAttributeAndContentCompression = context.getProperty(ALLOW_CONTENT_COMPRESSION).asBoolean();

        processGroup.getConnections().forEach(connection -> {
            final String strategy = connection.getLoadBalanceStrategy();

            if (LoadBalanceStrategy.DO_NOT_LOAD_BALANCE.name().equals(strategy) && !allowNone) {
                violations.add(new ConnectionViolation(connection,
                        LoadBalanceViolationType.DO_NOT_LOAD_BALANCE_DISALLOWED,
                        this.getClass().getSimpleName(),
                        connection.getLoadBalanceStrategy(),
                        context.getProperty(ALLOW_DO_NOT_LOAD_BALANCE).getValue()));
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
                // Connection load balancing is set to compress only attributes. Both attribute and attribute/content compression must be disallowed to cause a violation.
                if (LoadBalanceCompression.COMPRESS_ATTRIBUTES_ONLY.name().equals(compression) && !allowAttributeCompression && !allowAttributeAndContentCompression) {
                    violations.add(new ConnectionViolation(connection,
                            LoadBalanceViolationType.ATTRIBUTE_COMPRESSION_DISALLOWED,
                            this.getClass().getSimpleName(),
                            connection.getLoadBalanceCompression(),
                            context.getProperty(ALLOW_ATTRIBUTE_COMPRESSION).getValue()));
                }
                // Connection load balancing is set to compress both attributes and content. Only attribute/content compression must be disallowed to cause a violation.
                // (Disallowing attribute compression while allowing attribute/content compression is a configuration validation error and cannot exist.)
                if (LoadBalanceCompression.COMPRESS_ATTRIBUTES_AND_CONTENT.name().equals(compression) && !allowAttributeAndContentCompression) {
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

    private boolean isLoadBalancingAllowed(ValidationContext validationContext) {
        return validationContext.getProperty(ALLOW_PARTITION).asBoolean()
                || validationContext.getProperty(ALLOW_ROUND_ROBIN).asBoolean()
                || validationContext.getProperty(ALLOW_SINGLE_NODE).asBoolean();
    }

    private boolean isLoadBalancingEnabled(String strategy) {
        return LoadBalanceStrategy.PARTITION_BY_ATTRIBUTE.name().equals(strategy)
                || LoadBalanceStrategy.ROUND_ROBIN.name().equals(strategy)
                || LoadBalanceStrategy.SINGLE_NODE.name().equals(strategy);
    }

    private enum LoadBalanceViolationType implements ViolationType {

        DO_NOT_LOAD_BALANCE_DISALLOWED("NoLoadBalanceNotAllowed", "Load Balance Strategy", "is disallowed because \"" + ALLOW_DO_NOT_LOAD_BALANCE.getName() + "\" is set to "),
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
