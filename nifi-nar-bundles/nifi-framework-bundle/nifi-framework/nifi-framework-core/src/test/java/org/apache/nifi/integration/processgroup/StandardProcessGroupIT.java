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
package org.apache.nifi.integration.processgroup;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StandardProcessGroupIT extends FrameworkIntegrationTest {

    @Test
    public void testComponentsAffectedByVariableOverridden() {
        final ProcessGroup child = getFlowController().getFlowManager().createProcessGroup("child");
        child.setName("Child");
        child.setVariables(Collections.singletonMap("number", "5"));

        getRootGroup().setVariables(Collections.singletonMap("number", "1"));
        getRootGroup().addProcessGroup(child);

        final ProcessorNode processor = createProcessorNode(NumberRefProcessor.class);
        processor.setProperties(Collections.singletonMap(NumberRefProcessor.NUMBER.getName(), "${number}"));
        moveProcessor(processor, child);

        final Set<ComponentNode> componentsAffected = child.getComponentsAffectedByVariable("number");
        assertEquals(1, componentsAffected.size());
        assertTrue(componentsAffected.contains(processor));

        final Set<ComponentNode> rootAffected = getRootGroup().getComponentsAffectedByVariable("number");
        assertTrue(rootAffected.isEmpty());

        processor.setScheduldingPeriod("1 hour");
        child.startProcessor(processor, false);

        getRootGroup().setVariables(Collections.singletonMap("number", "2"));

        try {
            child.setVariables(Collections.singletonMap("number", "10"));
            Assert.fail("Updated variable that is referenced by a running processor");
        } catch (final IllegalStateException ise) {
            // Expected
        }

        child.stopProcessor(processor);
    }


    public static class NumberRefProcessor extends AbstractProcessor {
        static final PropertyDescriptor NUMBER = new Builder()
            .name("Number")
            .displayName("Number")
            .description("A Number")
            .required(true)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("1")
            .build();

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Collections.singletonList(NUMBER);
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        }
    }
}
