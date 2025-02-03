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
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;
import org.apache.nifi.flowanalysis.rules.util.ConnectionViolation;
import org.apache.nifi.flowanalysis.rules.util.FlowAnalysisRuleUtils;
import org.apache.nifi.flowanalysis.rules.util.ViolationType;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Tags({"connection", "backpressure"})
@CapabilityDescription("This rule will generate a violation if backpressure settings of a connection exceed configured thresholds. "
        + "Improper configuration of backpressure settings can lead to decreased performance because of excessive swapping and can "
        + "fill up the content repository with too much in-flight data.")
public class RestrictBackpressureSettings extends AbstractFlowAnalysisRule {

    public static final PropertyDescriptor COUNT_MIN = new PropertyDescriptor.Builder()
            .name("Minimum Backpressure Object Count Threshold")
            .description("This is the minimum value that should be set for the Object Count backpressure setting on connections. "
                    + "This can be used to prevent a user from setting a value of 0 which disables backpressure based on count.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor COUNT_MAX = new PropertyDescriptor.Builder()
            .name("Maximum Backpressure Object Count Threshold")
            .description("This is the maximum value that should be set for the Object Count backpressure setting on connections. "
                    + "This can be used to prevent a user from setting a very high value that may be leading to a lot of swapping.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    public static final PropertyDescriptor SIZE_MIN = new PropertyDescriptor.Builder()
            .name("Minimum Backpressure Data Size Threshold")
            .description("This is the minimum value that should be set for the Data Size backpressure setting on connections. "
                    + "This can be used to prevent a user from setting a value of 0 which disables backpressure based on size.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();

    public static final PropertyDescriptor SIZE_MAX = new PropertyDescriptor.Builder()
            .name("Maximum Backpressure Data Size Threshold")
            .description("This is the maximum value that should be set for the Data Size backpressure setting on connections. "
                    + "This can be used to prevent a user from setting a very high value that may be filling up the content repo.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 GB")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            COUNT_MIN,
            COUNT_MAX,
            SIZE_MIN,
            SIZE_MAX
    );

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final long minCount = validationContext.getProperty(COUNT_MIN).asLong();
        final long maxCount = validationContext.getProperty(COUNT_MAX).asLong();
        final double minSize = validationContext.getProperty(SIZE_MIN).asDataSize(DataUnit.B);
        final double maxSize = validationContext.getProperty(SIZE_MAX).asDataSize(DataUnit.B);

        if (minCount > maxCount) {
            results.add(
                    new ValidationResult.Builder()
                    .subject(COUNT_MIN.getName())
                    .valid(false)
                    .explanation("Value of '" + COUNT_MIN.getName() + "' cannot be strictly greater than '" + COUNT_MAX.getName() + "'")
                    .build());
        }
        if (Double.compare(minSize, maxSize) > 0) {
            results.add(
                    new ValidationResult.Builder()
                    .subject(SIZE_MIN.getName())
                    .valid(false)
                    .explanation("Value of '" + SIZE_MIN.getName() + "' cannot be strictly greater than '" + SIZE_MAX.getName() + "'")
                    .build());
        }

        return results;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup pg, FlowAnalysisRuleContext context) {
        final Collection<ConnectionViolation> violations = new ArrayList<>();

        final long minCount = context.getProperty(COUNT_MIN).asLong();
        final long maxCount = context.getProperty(COUNT_MAX).asLong();
        final double minSize = context.getProperty(SIZE_MIN).asDataSize(DataUnit.B);
        final double maxSize = context.getProperty(SIZE_MAX).asDataSize(DataUnit.B);

        pg.getConnections().forEach(
                connection -> {
                    if (connection.getBackPressureObjectThreshold() < minCount) {
                        violations.add(new ConnectionViolation(connection,
                                BackpressureViolationType.BP_COUNT_THRESHOLD_BELOW_LIMIT,
                                this.getClass().getSimpleName(),
                                connection.getBackPressureObjectThreshold().toString(),
                                context.getProperty(COUNT_MIN).getValue()));
                    }
                    if (connection.getBackPressureObjectThreshold() > maxCount) {
                        violations.add(new ConnectionViolation(connection,
                                BackpressureViolationType.BP_COUNT_THRESHOLD_ABOVE_LIMIT,
                                this.getClass().getSimpleName(),
                                connection.getBackPressureObjectThreshold().toString(),
                                context.getProperty(COUNT_MAX).getValue()));
                    }
                    final double sizeThreshold = DataUnit.parseDataSize(connection.getBackPressureDataSizeThreshold(), DataUnit.B);
                    if (Double.compare(sizeThreshold, minSize) < 0) {
                        violations.add(new ConnectionViolation(connection,
                                BackpressureViolationType.BP_SIZE_THRESHOLD_BELOW_LIMIT,
                                this.getClass().getSimpleName(),
                                connection.getBackPressureDataSizeThreshold(),
                                context.getProperty(SIZE_MIN).getValue()));
                    }
                    if (Double.compare(sizeThreshold, maxSize) > 0) {
                        violations.add(new ConnectionViolation(connection,
                                BackpressureViolationType.BP_SIZE_THRESHOLD_ABOVE_LIMIT,
                                this.getClass().getSimpleName(),
                                connection.getBackPressureDataSizeThreshold(),
                                context.getProperty(SIZE_MAX).getValue()));
                    }
                }
            );

        return FlowAnalysisRuleUtils.convertToGroupAnalysisResults(pg, violations);
    }

    private enum BackpressureViolationType implements ViolationType {

        BP_COUNT_THRESHOLD_BELOW_LIMIT("BackpressureCountThresholdTooLow", "Back Pressure Count Threshold", "cannot be less than"),
        BP_COUNT_THRESHOLD_ABOVE_LIMIT("BackpressureCountThresholdTooHigh", "Back Pressure Count Threshold", "cannot be greater than"),
        BP_SIZE_THRESHOLD_BELOW_LIMIT("BackpressureSizeThresholdTooLow", "Back Pressure Data Size Threshold", "cannot be less than"),
        BP_SIZE_THRESHOLD_ABOVE_LIMIT("BackpressureSizeThresholdTooHigh", "Back Pressure Data Size Threshold", "cannot be greater than");

        private final String id;
        private final String configurationItem;
        private final String violationMessage;

        BackpressureViolationType(String id, String configurationItem, String violationMessage) {
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
