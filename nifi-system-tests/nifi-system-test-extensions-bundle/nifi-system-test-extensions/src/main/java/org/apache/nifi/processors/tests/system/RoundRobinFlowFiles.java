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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nifi.processor.util.StandardValidators.POSITIVE_INTEGER_VALIDATOR;

public class RoundRobinFlowFiles extends AbstractProcessor {
    private volatile List<Relationship> relationships = new ArrayList<>();
    private final AtomicLong counter = new AtomicLong(0L);

    static final PropertyDescriptor RELATIONSHIP_COUNT = new Builder()
        .name("Number of Relationships")
        .displayName("Number of Relationships")
        .description("The number of Relationships")
        .required(true)
        .addValidator(POSITIVE_INTEGER_VALIDATOR)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(RELATIONSHIP_COUNT);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(relationships);
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        final List<Relationship> relationships = new ArrayList<>();
        for (int i = 1; i <= Integer.parseInt(newValue); i++) {
            relationships.add(createRelationship(i));
        }
        this.relationships = Collections.unmodifiableList(relationships);
    }

    private static Relationship createRelationship(final int num) {
        return new Relationship.Builder().name(String.valueOf(num))
            .description("Where to route flowfiles for this relationship index").build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long count = counter.getAndIncrement();
        final long numRelationships = context.getProperty(RELATIONSHIP_COUNT).asLong();
        final int relationshipIdx = (int) (count % numRelationships);
        final Relationship relationship = relationships.get(relationshipIdx);
        session.transfer(flowFile, relationship);
    }
}
