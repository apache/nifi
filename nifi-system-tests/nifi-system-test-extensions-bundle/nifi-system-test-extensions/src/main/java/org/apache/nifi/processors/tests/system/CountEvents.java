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

import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nifi.components.Validator.VALID;

@SupportsBatching
public class CountEvents extends AbstractSessionFactoryProcessor {
    private volatile ProcessSessionFactory sessionFactory;
    private final AtomicBoolean firstScheduleCounted = new AtomicBoolean(false);

    static final PropertyDescriptor NAME = new Builder()
        .name("Name")
        .displayName("Name")
        .description("Arbitrary Name")
        .required(false)
        .addValidator(VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    static final PropertyDescriptor SENSITIVE = new Builder()
        .name("Sensitive")
        .displayName("Sensitive")
        .description("Sensitive Property with no real meaning")
        .required(false)
        .addValidator(VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .sensitive(true)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(NAME, SENSITIVE);
    }

    @OnStopped
    public void onStopped() {
        sessionFactory.createSession().adjustCounter("Stopped", 1, true);
    }

    @OnScheduled
    public void onScheduled() {
        if (sessionFactory != null) {
            sessionFactory.createSession().adjustCounter("Scheduled", 1, true);
        }
    }

    @OnUnscheduled
    public void onUnScheduled() {
        if (sessionFactory != null) {
            sessionFactory.createSession().adjustCounter("UnScheduled", 1, true);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        this.sessionFactory = sessionFactory;

        final ProcessSession session = sessionFactory.createSession();
        if (!firstScheduleCounted.getAndSet(true)) {
            session.adjustCounter("Scheduled", 1, true);
        }

        session.adjustCounter("Triggered", 1, true);
    }
}
