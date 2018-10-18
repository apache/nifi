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
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"delay", "wait", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Delays a FlowFile. "
        + "Each FlowFile will be delayed per the 'Delay Duration' property of the processor.")
public class DelayFlowFile extends AbstractProcessor {
    public static final PropertyDescriptor DELAY_DURATION = new PropertyDescriptor.Builder()
            .name("delay-duration")
            .displayName("Delay Duration")
            .description("How long to delay the FlowFile for. Expected format is <duration> <time unit> "
                    + "where <duration> is a positive integer and time unit is one of milliseconds, seconds, minutes, hours")
            .required(true)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.MILLISECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully delayed the FlowFile").build();

    private Set<Relationship> relationships;
    protected List<PropertyDescriptor> propDescriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DELAY_DURATION);
        this.propDescriptors = Collections.unmodifiableList(descriptors);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        PropertyValue delayDuration = context.getProperty(DELAY_DURATION);

        // Find only FlowFile's that are no longer Delayed
        final List<FlowFile> flowFiles = session.get(new DelayFlowFileFilter(delayDuration));
        if (flowFiles.isEmpty()) {
            return;
        }

        session.transfer(flowFiles, REL_SUCCESS);
    }

    private class DelayFlowFileFilter implements FlowFileFilter {
        private final Long timeNow;
        private final PropertyValue delayDuration;
        private final boolean hasEL;
        private final Long fixedDelay;

        public DelayFlowFileFilter(PropertyValue delayDuration){
            this.delayDuration = delayDuration;

            //If there is no EL, read the fixed delay
            this.hasEL = this.delayDuration.isExpressionLanguagePresent();
            this.fixedDelay = !hasEL?delayDuration.asTimePeriod(TimeUnit.MILLISECONDS).longValue():0;

            final Calendar now = Calendar.getInstance();
            this.timeNow = now.getTimeInMillis();
        }

        @Override
        public FlowFileFilterResult filter(FlowFile flowFile) {
            final Long queuedDate = flowFile.getLastQueueDate();
            final Long currentDelay = timeNow - queuedDate;

            Long delay = fixedDelay;

            if(hasEL){
                delay = delayDuration.evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS).longValue();
            }

            return currentDelay >= delay? FlowFileFilterResult.ACCEPT_AND_CONTINUE:FlowFileFilterResult.REJECT_AND_CONTINUE;
        }
    }
}