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
import org.apache.nifi.cs.tests.system.SleepService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
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

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(VALIDATE_SLEEP_TIME);
        properties.add(ON_SCHEDULED_SLEEP_TIME);
        properties.add(ON_TRIGGER_SLEEP_TIME);
        properties.add(ON_STOPPED_SLEEP_TIME);
        properties.add(SLEEP_SERVICE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final long sleepMillis = validationContext.getProperty(VALIDATE_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        sleep(sleepMillis);

        return Collections.emptyList();
    }

    private void sleep(final long millis) {
        if (millis > 0L) {
            try {
                Thread.sleep(millis);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @OnScheduled
    public void onEnabled(final ProcessContext context) {
        sleep(context.getProperty(ON_SCHEDULED_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS));
    }

    @OnStopped
    public void onDisabled(final ProcessContext context) {
        sleep(context.getProperty(ON_STOPPED_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS));
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final long sleepMillis = context.getProperty(ON_TRIGGER_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        sleep(sleepMillis);

        final SleepService service = context.getProperty(SLEEP_SERVICE).asControllerService(SleepService.class);
        if (service != null) {
            service.sleep();
        }

        FlowFile flowFile = session.get();
        if (flowFile != null) {
            session.transfer(flowFile, REL_SUCCESS);
        }
    }
}
