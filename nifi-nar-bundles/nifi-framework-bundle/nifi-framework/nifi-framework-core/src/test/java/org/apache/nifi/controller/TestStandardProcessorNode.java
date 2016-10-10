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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Test;

public class TestStandardProcessorNode {

    @Test(timeout = 10000)
    public void testStart() throws InterruptedException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestStandardProcessorNode.class.getResource("/conf/nifi.properties").getFile());
        final ProcessorThatThrowsExceptionOnScheduled processor = new ProcessorThatThrowsExceptionOnScheduled();
        final String uuid = UUID.randomUUID().toString();

        final StandardProcessorNode procNode = new StandardProcessorNode(processor, uuid, createValidationContextFactory(), null, null, NiFiProperties.createBasicNiFiProperties(null, null));
        final ScheduledExecutorService taskScheduler = new FlowEngine(2, "TestStandardProcessorNode", true);

        final StandardProcessContext processContext = new StandardProcessContext(procNode, null, null, null, null);
        final SchedulingAgentCallback schedulingAgentCallback = new SchedulingAgentCallback() {
            @Override
            public void postMonitor() {
            }

            @Override
            public Future<?> invokeMonitoringTask(final Callable<?> task) {
                return taskScheduler.submit(task);
            }

            @Override
            public void trigger() {
                Assert.fail("Should not have completed");
            }
        };

        procNode.start(taskScheduler, 20000L, processContext, schedulingAgentCallback);

        Thread.sleep(1000L);
        assertEquals(1, processor.onScheduledCount);
        assertEquals(1, processor.onUnscheduledCount);
        assertEquals(1, processor.onStoppedCount);
    }


    private ValidationContextFactory createValidationContextFactory() {
        return new ValidationContextFactory() {
            @Override
            public ValidationContext newValidationContext(Map<PropertyDescriptor, String> properties, String annotationData, String groupId, String componentId) {
                return new ValidationContext() {

                    @Override
                    public ControllerServiceLookup getControllerServiceLookup() {
                        return null;
                    }

                    @Override
                    public ValidationContext getControllerServiceValidationContext(ControllerService controllerService) {
                        return null;
                    }

                    @Override
                    public ExpressionLanguageCompiler newExpressionLanguageCompiler() {
                        return null;
                    }

                    @Override
                    public PropertyValue getProperty(PropertyDescriptor property) {
                        return newPropertyValue(properties.get(property));
                    }

                    @Override
                    public PropertyValue newPropertyValue(String value) {
                        return new MockPropertyValue(value);
                    }

                    @Override
                    public Map<PropertyDescriptor, String> getProperties() {
                        return Collections.unmodifiableMap(properties);
                    }

                    @Override
                    public String getAnnotationData() {
                        return null;
                    }

                    @Override
                    public boolean isValidationRequired(ControllerService service) {
                        return false;
                    }

                    @Override
                    public boolean isExpressionLanguagePresent(String value) {
                        return false;
                    }

                    @Override
                    public boolean isExpressionLanguageSupported(String propertyName) {
                        return false;
                    }

                    @Override
                    public String getProcessGroupIdentifier() {
                        return groupId;
                    }
                };
            }

            @Override
            public ValidationContext newValidationContext(Set<String> serviceIdentifiersToNotValidate, Map<PropertyDescriptor, String> properties, String annotationData, String groupId,
                String componentId) {
                return newValidationContext(properties, annotationData, groupId, componentId);
            }
        };

    }


    public static class ProcessorThatThrowsExceptionOnScheduled extends AbstractProcessor {
        private int onScheduledCount = 0;
        private int onUnscheduledCount = 0;
        private int onStoppedCount = 0;

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }

        @OnScheduled
        public void onScheduled() {
            onScheduledCount++;
            throw new ProcessException("OnScheduled called - Unit Test throws Exception intentionally");
        }

        @OnUnscheduled
        public void onUnscheduled() {
            onUnscheduledCount++;
        }

        @OnStopped
        public void onStopped() {
            onStoppedCount++;
        }
    }
}
