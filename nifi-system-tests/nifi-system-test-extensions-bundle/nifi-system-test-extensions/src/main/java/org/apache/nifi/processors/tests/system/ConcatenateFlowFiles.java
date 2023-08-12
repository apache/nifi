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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConcatenateFlowFiles extends AbstractSessionFactoryProcessor {
    static final PropertyDescriptor FLOWFILE_COUNT = new Builder()
        .name("FlowFile Count")
        .displayName("FlowFile Count")
        .description("Number of FlowFiles to concatenate together")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    static final Relationship ORIGINAL = new Relationship.Builder()
        .name("original")
        .build();
    static final Relationship MERGED = new Relationship.Builder()
        .name("merged")
        .build();

    private int flowFileCount;
    private List<FlowFile> flowFiles;
    private ProcessSession mergeSession;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(FLOWFILE_COUNT);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(ORIGINAL, MERGED));
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        flowFileCount = context.getProperty(FLOWFILE_COUNT).asInteger();
        flowFiles = new ArrayList<>();
    }

    @OnStopped
    public void reset() {
        flowFiles.clear();
        mergeSession.rollback();
        mergeSession = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession pollSession = sessionFactory.createSession();
        FlowFile flowFile = pollSession.get();
        if (flowFile == null) {
            return;
        }

        if (mergeSession == null) {
            mergeSession = sessionFactory.createSession();
        }

        pollSession.migrate(mergeSession, Collections.singleton(flowFile));
        flowFiles.add(flowFile);

        if (flowFiles.size() == flowFileCount) {
            FlowFile merged = mergeSession.create(flowFiles);
            try (final OutputStream out = mergeSession.write(merged)) {
                for (final FlowFile input : flowFiles) {
                    try (final InputStream in = mergeSession.read(input)) {
                        StreamUtils.copy(in, out);
                    }
                }
            } catch (final Exception e) {
                mergeSession.rollback();
                throw new ProcessException("Failed to merge", e);
            }

            mergeSession.transfer(merged, MERGED);
            mergeSession.transfer(flowFiles, ORIGINAL);
            flowFiles.clear();
            mergeSession.commitAsync();
        } else {
            context.yield();
        }
    }

}
