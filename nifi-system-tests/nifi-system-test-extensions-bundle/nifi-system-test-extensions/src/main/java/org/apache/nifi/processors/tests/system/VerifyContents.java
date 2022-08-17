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
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class VerifyContents extends AbstractProcessor {
    private static final Relationship REL_UNMATCHED = new Relationship.Builder()
        .name("unmatched")
        .build();

    private final AtomicReference<Set<Relationship>> relationshipsRef = new AtomicReference<>(Collections.singleton(REL_UNMATCHED));

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .dynamic(true)
            .addValidator(Validator.VALID)
            .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        final Relationship relationship = new Relationship.Builder()
            .name(descriptor.getName())
            .build();

        final Set<Relationship> updatedRelationships = new HashSet<>(relationshipsRef.get());

        if (newValue == null) {
            updatedRelationships.remove(relationship);
        } else {
            updatedRelationships.add(relationship);
        }

        updatedRelationships.add(REL_UNMATCHED); // Ensure that the unmatched relationship is always available
        relationshipsRef.set(updatedRelationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String contents;
        try (final InputStream in = session.read(flowFile);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            StreamUtils.copy(in, baos);
            contents = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        } catch (final Exception e) {
            throw new ProcessException(e);
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final String propertyName = entry.getKey().getName();
            if (contents.equals(entry.getValue())) {
                getLogger().info("Routing {} to {}", flowFile, propertyName);
                session.transfer(flowFile, new Relationship.Builder().name(propertyName).build());
                return;
            }
        }

        getLogger().info("Routing {} to {}", flowFile, REL_UNMATCHED);
        session.transfer(flowFile, REL_UNMATCHED);
    }
}
