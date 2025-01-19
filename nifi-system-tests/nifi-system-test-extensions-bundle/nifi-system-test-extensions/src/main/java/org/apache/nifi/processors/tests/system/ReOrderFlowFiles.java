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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;

@CapabilityDescription("Selects FlowFiles that match the given criteria and transfers them to the 'success' relationship. Then, selects all other FlowFiles and transfers them " +
                       "to the success relationship. Note that this Processor will not work properly if it is scheduled to run while its incoming queue(s) are being populated. " +
                       "This is meant to be used only for purposes of testing in a Stateless execution engine and makes use of FlowFileFilters.")
public class ReOrderFlowFiles extends AbstractProcessor {
    protected static PropertyDescriptor FIRST_SELECTION_CRITERIA = new PropertyDescriptor.Builder()
        .name("First Group Selection Criteria")
        .description("An Expression Language expression that evaluates to true or false. FlowFiles that evaluate to true will be transferred first; others " +
                     "will be transferred after.")
        .required(true)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.BOOLEAN, false))
        .build();

    private static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles are transferred to this relationship.")
        .build();

    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS);
    private static final List<PropertyDescriptor> properties = List.of(FIRST_SELECTION_CRITERIA);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final PropertyValue selectionValue = context.getProperty(FIRST_SELECTION_CRITERIA);

        final List<FlowFile> matching = session.get(flowFile -> {
            final boolean selected = selectionValue.evaluateAttributeExpressions(flowFile).asBoolean();
            return selected ? FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE : FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;
        });

        final List<FlowFile> unmatched = session.get(flowFile -> FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE);

        session.transfer(matching, REL_SUCCESS);
        session.transfer(unmatched, REL_SUCCESS);
    }
}
