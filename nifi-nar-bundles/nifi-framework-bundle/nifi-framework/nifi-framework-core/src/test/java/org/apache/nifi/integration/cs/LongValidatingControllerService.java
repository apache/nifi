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
package org.apache.nifi.integration.cs;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

public class LongValidatingControllerService extends AbstractControllerService {
    public static final PropertyDescriptor DELAY = new Builder()
        .name("Delay")
        .displayName("Delay")
        .description("Amount of time to sleep during validation")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .defaultValue("2 sec")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(DELAY);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final long millis = validationContext.getProperty(DELAY).asTimePeriod(TimeUnit.MILLISECONDS);
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ie) {
        }

        return Collections.emptyList();
    }
}
