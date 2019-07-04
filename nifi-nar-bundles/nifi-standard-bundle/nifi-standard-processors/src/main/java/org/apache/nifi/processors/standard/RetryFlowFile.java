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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"Retry", "FlowFile"})
@CapabilityDescription("FlowFiles passed to this Processor have a 'Retry Attribute' value checked against a " +
        "configured 'Maximum Retries' value. If the current attribute value is below the configured maximum, the " +
        "FlowFile is passed to a retry relationship. The FlowFile may or may not be penalized in that condition. " +
        "If the FlowFile's attribute value exceeds the configured maximum, the FlowFile will be passed to a " +
        "'retries_exceeded' relationship. WARNING: If the incoming FlowFile has a non-numeric value in the " +
        "configured 'Retry Attribute' attribute, it will be reset to '1'. You may choose to fail the FlowFile " +
        "instead of performing the reset. Additional dynamic properties can be defined for any attributes you " +
        "wish to add to the FlowFiles transferred to 'retries_exceeded'. These attributes support attribute " +
        "expression language.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@SideEffectFree
@DefaultSettings(penaltyDuration = "2 min")
@ReadsAttribute(attribute = "Retry Attribute",
        description = "Will read the attribute or attribute expression language result as defined in 'Retry Attribute'")
@WritesAttribute(attribute = "Retry Attribute",
        description = "User defined retry attribute is updated with the current retry count")
@DynamicProperty(name = "Exceeded FlowFile Attribute Key",
        value = "The value of the attribute added to the FlowFile",
        description = "One or more dynamic properties can be used to add attributes to FlowFiles passed to " +
                "the 'retries_exceeded' relationship",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
public class RetryFlowFile extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private String retryAttribute;
    private Integer maximumRetries;
    private Boolean penalizeRetried;
    private Boolean failOnOverwrite;

    public static final PropertyDescriptor RETRY_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Retry Attribute")
            .description("The name of the attribute that contains the current retry count for the FlowFile. " +
                    "WARNING: If the name matches an attribute already on the FlowFile that does not contain a " +
                    "numerical value, the processor will either overwrite that attribute with '1' or fail " +
                    "based on configuration.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("flowfile.retries")
            .build();
    public static final PropertyDescriptor MAXIMUM_RETRIES = new PropertyDescriptor.Builder()
            .name("Maximum Retries")
            .description("The maximum number of times a FlowFile can be retried before being " +
                    "passed to the 'retries_exceeded' relationship")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, Integer.MAX_VALUE, true))
            .defaultValue("3")
            .build();
    public static final PropertyDescriptor PENALIZE_RETRIED = new PropertyDescriptor.Builder()
            .name("Penalize Retries")
            .description("If set to 'true', this Processor will penalize input FlowFiles before passing them " +
                    "to the 'retry' relationship. This does not apply to the 'retries_exceeded' relationship.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor FAIL_ON_OVERWRITE = new PropertyDescriptor.Builder()
            .name("Fail on Non-numerical Overwrite")
            .description("If the FlowFile already has the attribute defined in 'Retry Attribute' that is " +
                    "*not* a number, fail the FlowFile instead of resetting that value to '1'")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship RETRY = new Relationship.Builder()
            .name("retry")
            .description("Input FlowFile has not exceeded the configured maximum retry count, pass this " +
                    "relationship back to the input Processor to create a limited feedback loop.")
            .build();
    public static final Relationship RETRIES_EXCEEDED = new Relationship.Builder()
            .name("retries_exceeded")
            .description("Input FlowFile has exceeded the configured maximum retry count, do not pass this " +
                    "relationship back to the input Processor to terminate the limited feedback loop.")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The processor is configured such that a non-numerical value on 'Retry Attribute' " +
                    "results in a failure instead of resetting that value to '1'. This will immediately " +
                    "terminate the limited feedback loop.")
            .autoTerminateDefault(true)
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(RETRY_ATTRIBUTE);
        props.add(MAXIMUM_RETRIES);
        props.add(PENALIZE_RETRIED);
        props.add(FAIL_ON_OVERWRITE);
        this.properties = Collections.unmodifiableList(props);

        Set<Relationship> rels = new HashSet<>();
        rels.add(RETRY);
        rels.add(RETRIES_EXCEEDED);
        rels.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Attribute " + propertyDescriptorName + " will be placed on FlowFiles " +
                        "exceeding the retry count")
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    @SuppressWarnings("unused")
    public void onScheduled(final ProcessContext context) {
        maximumRetries = context.getProperty(MAXIMUM_RETRIES).asInteger();
        penalizeRetried = context.getProperty(PENALIZE_RETRIED).asBoolean();
        retryAttribute = context.getProperty(RETRY_ATTRIBUTE).evaluateAttributeExpressions().getValue();
        failOnOverwrite = context.getProperty(FAIL_ON_OVERWRITE).asBoolean();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowfile = session.get();
        if (null == flowfile || flowfile.isPenalized())
            return;

        String retryAttributeValue = flowfile.getAttribute(retryAttribute);
        Integer currentRetry;
        try {
            currentRetry = (null == retryAttributeValue)
                    ? 1
                    : Integer.valueOf(retryAttributeValue.trim()) + 1;
        } catch (NumberFormatException ex) {
            // Configured to fail if this was not a number
            if (failOnOverwrite) {
                session.transfer(flowfile, FAILURE);
                return;
            }
            // reset to '1' if that wasn't the case
            currentRetry = 1;
        }

        if (currentRetry > maximumRetries) {
            // Add dynamic properties
            for (PropertyDescriptor descriptor : context.getProperties().keySet()) {
                if (!descriptor.isDynamic())
                    continue;

                String value = context.getProperty(descriptor)
                        .evaluateAttributeExpressions(flowfile)
                        .getValue();
                if (null != value && !StringUtils.isBlank(value))
                    flowfile = session.putAttribute(flowfile, descriptor.getName(), value);
            }

            session.transfer(flowfile, RETRIES_EXCEEDED);
        } else {
            if (penalizeRetried)
                session.penalize(flowfile);

            // Update and transfer
            flowfile = session.putAttribute(flowfile, retryAttribute, String.valueOf(currentRetry));
            session.transfer(flowfile, RETRY);
        }
    }
}
