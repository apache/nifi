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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.cs.tests.system.SleepService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Sleep extends AbstractProcessor {
    public static final PropertyDescriptor VALIDATE_SLEEP_TIME = new Builder()
        .name("Validate Sleep Time")
        .description("The amount of time to sleep during validation")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 sec")
        .build();
    public static final PropertyDescriptor ON_TRIGGER_SLEEP_TIME = new Builder()
        .name("onTrigger Sleep Time")
        .description("The amount of time to sleep during each trigger")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 sec")
        .build();
    public static final PropertyDescriptor ON_SCHEDULED_SLEEP_TIME = new Builder()
        .name("@OnScheduled Sleep Time")
        .description("The amount of time to sleep when scheduled")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 sec")
        .build();
    public static final PropertyDescriptor ON_STOPPED_SLEEP_TIME = new Builder()
        .name("@OnStopped Sleep Time")
        .description("The amount of time to sleep when stopped")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 sec")
        .build();
    static final PropertyDescriptor SLEEP_SERVICE = new Builder()
        .name("Sleep Service")
        .description("Controller Service that sleeps")
        .required(false)
        .identifiesControllerService(SleepService.class)
        .build();
    static final PropertyDescriptor IGNORE_INTERRUPTS = new Builder()
        .name("Ignore Interrupts")
        .description("If true, the processor will not respond to interrupts while sleeping.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();
    static final PropertyDescriptor STOP_SLEEPING_WHEN_UNSCHEDULED = new Builder()
        .name("Stop Sleeping When Unscheduled")
        .description("If true, the processor will stop sleeping whenever the processor is unscheduled. " +
            "If false, the processor will continue sleeping until the sleep time has elapsed. This property only applies to the " +
            "onTrigger Sleep Time.")
        .required(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    private static final List<PropertyDescriptor> properties = List.of(
        VALIDATE_SLEEP_TIME,
        ON_SCHEDULED_SLEEP_TIME,
        ON_TRIGGER_SLEEP_TIME,
        ON_STOPPED_SLEEP_TIME,
        SLEEP_SERVICE,
        IGNORE_INTERRUPTS,
        STOP_SLEEPING_WHEN_UNSCHEDULED
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final long sleepMillis = validationContext.getProperty(VALIDATE_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        sleep(sleepMillis, isIgnoreInterrupt(validationContext), false);

        return Collections.emptyList();
    }

    private void sleep(final long millis, final boolean ignoreInterrupts, final boolean stopSleepOnUnscheduled) {
        if (millis == 0L) {
            return;
        }

        final long stopTime = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < stopTime) {
            if (stopSleepOnUnscheduled && !isScheduled()) {
                return;
            }

            // Sleep for up to the stopTime but no more than 50 milliseconds at a time. This gives us a chance
            // to periodically check if the processor is still scheduled
            final long sleepMillis = Math.min(stopTime - System.currentTimeMillis(), 50L);
            try {
                Thread.sleep(sleepMillis);
            } catch (final InterruptedException e) {
                if (ignoreInterrupts) {
                    continue;
                }

                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @OnScheduled
    public void onEnabled(final ProcessContext context) {
        sleep(context.getProperty(ON_SCHEDULED_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS), isIgnoreInterrupt(context), false);
    }

    private boolean isIgnoreInterrupt(final PropertyContext context) {
        return context.getProperty(IGNORE_INTERRUPTS).asBoolean();
    }

    @OnStopped
    public void onDisabled(final ProcessContext context) {
        sleep(context.getProperty(ON_STOPPED_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS), isIgnoreInterrupt(context), false);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long sleepMillis = context.getProperty(ON_TRIGGER_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        final boolean ignoreInterrupts = isIgnoreInterrupt(context);
        final boolean stopSleepOnUnscheduled = context.getProperty(STOP_SLEEPING_WHEN_UNSCHEDULED).asBoolean();
        sleep(sleepMillis, ignoreInterrupts, stopSleepOnUnscheduled);

        final SleepService service = context.getProperty(SLEEP_SERVICE).asControllerService(SleepService.class);
        if (service != null) {
            service.sleep();
        }

        getLogger().info("Finished onTrigger sleep");
        session.transfer(flowFile, REL_SUCCESS);
    }
}
