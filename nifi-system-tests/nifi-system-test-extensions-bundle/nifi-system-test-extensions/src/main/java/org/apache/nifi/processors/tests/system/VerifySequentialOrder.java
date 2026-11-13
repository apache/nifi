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

package org.apache.nifi.processors.tests.system;

import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;

@TriggerSerially
@CapabilityDescription("Pulls one FlowFile at a time from the incoming queue and routes it to 'sequential' if its Value to Consider is exactly one greater than the "
    + "Value to Consider of the previous FlowFile that this Processor received. Otherwise, the FlowFile is routed to 'failure'. The first FlowFile that the Processor "
    + "receives is always routed to 'sequential'.")
public class VerifySequentialOrder extends AbstractProcessor {

    static final PropertyDescriptor VALUE_TO_CONSIDER = new PropertyDescriptor.Builder()
        .name("Value to Consider")
        .description("The whole number to compare against the Value to Consider of the previous FlowFile")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final Relationship REL_SEQUENTIAL = new Relationship.Builder()
        .name("sequential")
        .description("The first FlowFile, and any FlowFile whose Value to Consider is exactly one greater than that of the previous FlowFile")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile whose Value to Consider is not exactly one greater than that of the previous FlowFile, or is not a whole number")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(VALUE_TO_CONSIDER);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SEQUENTIAL, REL_FAILURE);

    private volatile Long previousValue;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String value = context.getProperty(VALUE_TO_CONSIDER).evaluateAttributeExpressions(flowFile).getValue();
        final long currentValue;
        try {
            currentValue = Long.parseLong(value);
        } catch (final NumberFormatException e) {
            getLogger().error("Routing {} to failure because its Value to Consider [{}] is not a whole number", flowFile, value);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Long lastValue = previousValue;
        previousValue = currentValue;

        if (lastValue == null || currentValue == lastValue + 1) {
            session.transfer(flowFile, REL_SEQUENTIAL);
        } else {
            getLogger().warn("Routing {} to failure because its Value to Consider is {} but the previous FlowFile's value was {}", flowFile, currentValue, lastValue);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
