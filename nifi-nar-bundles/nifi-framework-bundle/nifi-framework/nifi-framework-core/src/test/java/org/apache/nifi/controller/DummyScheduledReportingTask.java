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

package org.apache.nifi.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.scheduling.SchedulingStrategy;

/**
 * Dummy reporting task to test @DefaultSchedule annotation
 */
@DefaultSchedule(strategy = SchedulingStrategy.CRON_DRIVEN, period = "0 0 0 1/1 * ?")
public class DummyScheduledReportingTask extends AbstractReportingTask {

    public static final PropertyDescriptor TEST_WITH_DEFAULT_VALUE = new PropertyDescriptor.Builder()
        .name("Test with default value")
        .description("Test with default value")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("nifi")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor TEST_WITHOUT_DEFAULT_VALUE = new PropertyDescriptor.Builder()
        .name("Test without default value")
        .description("Test without default value")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(TEST_WITH_DEFAULT_VALUE);
        properties.add(TEST_WITHOUT_DEFAULT_VALUE);
        return properties;
    }

    @Override
    public void onTrigger(ReportingContext context) {

    }
}