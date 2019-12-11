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
package org.apache.nifi.stateless.core;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Queues;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.FlowFileAcquisitionMethod;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.glassfish.jersey.internal.guava.Sets;
import org.junit.Test;

public class TestProvenanceCollector {

    @Test
    public void testFlowFileAcquisitionMethodSetOnReceiveFetch() {
        final Set<ProvenanceEventRecord> events = Sets.newHashSet();
        final StatelessProcessSession processSession = new StatelessProcessSession(Queues.newLinkedBlockingQueue(),
                events, new DummyProcessor(), Sets.newHashSet(), false, () -> {});
        final ProvenanceCollector collector = new ProvenanceCollector(processSession, events, "processorId", "processorType");

        final StatelessFlowFile flowFile = processSession.create();
        collector.receive(flowFile, "transitUri", "sourceSystemId", "details", FlowFileAcquisitionMethod.ACTIVE_QUERY, 100L);
        collector.fetch(flowFile, "transitUri", "details", FlowFileAcquisitionMethod.ACTIVE_QUERY, 100L);

        assertEquals(2, events.size());
        events.forEach(
                event -> assertEquals(FlowFileAcquisitionMethod.ACTIVE_QUERY, event.getFlowFileAcquisitionMethod())
        );
    }

    private static class DummyProcessor extends AbstractProcessor {
        static final PropertyDescriptor REQUIRED_PROP = new PropertyDescriptor.Builder()
                .name("required")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
        static final PropertyDescriptor DEFAULTED_PROP = new PropertyDescriptor.Builder()
                .name("defaulted")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .defaultValue("default-value")
                .build();
        static final PropertyDescriptor OPTIONAL_PROP = new PropertyDescriptor.Builder()
                .name("optional")
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        private final Map<PropertyDescriptor, Integer> propertyModifiedCount = new HashMap<>();

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            final List<PropertyDescriptor> properties = new ArrayList<>();
            properties.add(REQUIRED_PROP);
            properties.add(DEFAULTED_PROP);
            properties.add(OPTIONAL_PROP);
            return properties;
        }

        @Override
        public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
            Integer updateCount = propertyModifiedCount.get(descriptor);
            if (updateCount == null) {
                updateCount = 0;
            }

            propertyModifiedCount.put(descriptor, updateCount + 1);
        }

        public int getUpdateCount(final PropertyDescriptor descriptor) {
            Integer updateCount = propertyModifiedCount.get(descriptor);
            return (updateCount == null) ? 0 : updateCount;
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        }
    }
}
