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
package org.apache.nifi.web.search.attributematchers;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ProcessorNode;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class ProcessorMetadataMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private ProcessorNode processorNode;

    @Test
    public void testMatching() {
        // given
        final ProcessorMetadataMatcher testSubject = new ProcessorMetadataMatcher();
        final Processor processor = new LoremProcessor();
        Mockito.when(processorNode.getProcessor()).thenReturn(processor);
        Mockito.when(processorNode.getComponentType()).thenReturn("Lorem");

        // when
        testSubject.match(processorNode, searchQuery, matches);

        // then
        thenMatchConsistsOf("Type: LoremProcessor", "Type: Lorem");
    }

    private static class LoremProcessor implements Processor {

        @Override
        public void initialize(ProcessorInitializationContext context) {
            // noop
        }

        @Override
        public Set<Relationship> getRelationships() {
            return null;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
            // noop
        }

        @Override
        public Collection<ValidationResult> validate(ValidationContext context) {
            return null;
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String name) {
            return null;
        }

        @Override
        public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
            // noop
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return null;
        }

        @Override
        public String getIdentifier() {
            return null;
        }
    }
}