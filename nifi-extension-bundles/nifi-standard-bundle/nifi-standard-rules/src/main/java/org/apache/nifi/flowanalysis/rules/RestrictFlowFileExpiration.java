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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.FormatUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({"connection", "expiration", "age"})
@CapabilityDescription("This rule will generate a violation if FlowFile expiration settings of a connection exceed configured thresholds. "
        + "Improper configuration of FlowFile expiration settings can cause files to be deleted unexpectedly and can cause the content "
        + "repository to fill up.")
public class RestrictFlowFileExpiration extends AbstractFlowAnalysisRule {
    public static final PropertyDescriptor ALLOW_ZERO = new PropertyDescriptor.Builder()
            .name("Allow Zero Expiration")
            .description("If set to true, a 0 second FlowFile Expiration on connections is allowed despite other configured restrictions."
                    + " If set to false, a 0 second FlowFile Expiration will be compared against the other configured restrictions."
                    + " This can be used to prevent a user from setting a value of 0 seconds which could fill up"
                    + " the content repository if files accumulate in front of stopped processors.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor MIN_FLOWFILE_EXPIRATION = new PropertyDescriptor.Builder()
            .name("Minimum FlowFile Expiration")
            .description("This is the minimum value that should be set for the FlowFile Expiration setting on connections."
                    + " This can be used to prevent a user from setting a very small expiration which can cause files to be"
                    + " deleted unexpectedly.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 min")
            .build();

    public static final PropertyDescriptor MAX_FLOWFILE_EXPIRATION = new PropertyDescriptor.Builder()
            .name("Maximum FlowFile Expiration")
            .description("This is the maximum value that should be set for the FlowFile Expiration setting on connections."
                    + " This can be used to prevent a user from setting a large expiration which could fill up the content"
                    + " repository if files accumulate in front of stopped processors.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 days")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ALLOW_ZERO,
            MIN_FLOWFILE_EXPIRATION,
            MAX_FLOWFILE_EXPIRATION
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final long minSize = validationContext.getProperty(MIN_FLOWFILE_EXPIRATION).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maxSize = validationContext.getProperty(MAX_FLOWFILE_EXPIRATION).asTimePeriod(TimeUnit.MILLISECONDS);

        if (minSize > maxSize) {
            results.add(
                    new ValidationResult.Builder()
                            .subject(MIN_FLOWFILE_EXPIRATION.getName())
                            .valid(false)
                            .explanation("Value of '" + MIN_FLOWFILE_EXPIRATION.getName() + "' cannot be greater than '" + MAX_FLOWFILE_EXPIRATION.getName() + "'")
                            .build());
        }

        return results;
    }

    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup pg, FlowAnalysisRuleContext context) {
        final Collection<ConnectionViolation> violations = new ArrayList<>();

        final boolean allowZero = context.getProperty(ALLOW_ZERO).asBoolean();
        final long minSize = context.getProperty(MIN_FLOWFILE_EXPIRATION).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maxSize = context.getProperty(MAX_FLOWFILE_EXPIRATION).asTimePeriod(TimeUnit.MILLISECONDS);

        pg.getConnections().forEach(connection -> {
            final long connectionExpiration = FormatUtils.getTimeDuration(connection.getFlowFileExpiration(), TimeUnit.MILLISECONDS);

            if (connectionExpiration != 0 || !allowZero) {
                if (connectionExpiration < minSize) {
                    violations.add(new ConnectionViolation(connection,
                            ExpirationViolationType.EXPIRATION_BELOW_MINIMUM,
                            this.getClass().getSimpleName(),
                            connection.getFlowFileExpiration(),
                            context.getProperty(MIN_FLOWFILE_EXPIRATION).getValue()));
                }
                if (connectionExpiration > maxSize) {
                    violations.add(new ConnectionViolation(connection,
                            ExpirationViolationType.EXPIRATION_ABOVE_MAXIMUM,
                            this.getClass().getSimpleName(),
                            connection.getFlowFileExpiration(),
                            context.getProperty(MAX_FLOWFILE_EXPIRATION).getValue()));
                }
            }
        });

        return FlowAnalysisRuleUtils.convertToGroupAnalysisResults(pg, violations);
    }

    private enum ExpirationViolationType implements ViolationType {

        EXPIRATION_BELOW_MINIMUM("FlowFileExpirationTooLow", "FlowFile Expiration", "cannot be less than"),
        EXPIRATION_ABOVE_MAXIMUM("FlowFileExpirationTooHigh", "FlowFile Expiration", "cannot be greater than");

        private final String id;
        private final String configurationItem;
        private final String violationMessage;

        ExpirationViolationType(String id, String configurationItem, String violationMessage) {
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
