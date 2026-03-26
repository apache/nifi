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

package org.apache.nifi.components.connector.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"test", "sleep", "delay", "timing"})
@CapabilityDescription("Test processor that introduces configurable delays during different lifecycle events. " +
    "Can sleep during OnScheduled, OnStopped, and/or onTrigger methods.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
public class Sleep extends AbstractProcessor {

    static final PropertyDescriptor SLEEP_DURATION = new PropertyDescriptor.Builder()
        .name("Sleep Duration")
        .displayName("Sleep Duration")
        .description("Length of time to sleep when enabled.")
        .required(true)
        .defaultValue("5 sec")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    static final PropertyDescriptor SLEEP_ON_SCHEDULED = new PropertyDescriptor.Builder()
        .name("Sleep On Scheduled")
        .displayName("Sleep On Scheduled")
        .description("Sleep during OnScheduled when enabled.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final PropertyDescriptor SLEEP_ON_TRIGGER = new PropertyDescriptor.Builder()
        .name("Sleep On Trigger")
        .displayName("Sleep On Trigger")
        .description("Sleep during onTrigger when enabled.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final PropertyDescriptor SLEEP_ON_STOPPED = new PropertyDescriptor.Builder()
        .name("Sleep On Stopped")
        .displayName("Sleep On Stopped")
        .description("Sleep during OnStopped when enabled.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles are routed to success after optional sleeping.")
        .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
        SLEEP_DURATION,
        SLEEP_ON_SCHEDULED,
        SLEEP_ON_TRIGGER,
        SLEEP_ON_STOPPED
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (context.getProperty(SLEEP_ON_SCHEDULED).asBoolean()) {
            sleep(context);
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        if (context.getProperty(SLEEP_ON_STOPPED).asBoolean()) {
            sleep(context);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        if (context.getProperty(SLEEP_ON_TRIGGER).asBoolean()) {
            sleep(context);
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    private void sleep(final ProcessContext context) {
        final long durationMillis = context.getProperty(SLEEP_DURATION).asTimePeriod(TimeUnit.MILLISECONDS);
        final ComponentLog logger = getLogger();
        try {
            if (durationMillis > 0) {
                Thread.sleep(durationMillis);
            }
        } catch (final InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            logger.warn("Sleep processor interrupted while sleeping for {} ms", durationMillis);
        }
    }
}
