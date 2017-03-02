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
package org.apache.nifi.test.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGeneratorTestProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();

    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorTestProcessor.class);

    private final Set<Relationship> relationships;

    public DataGeneratorTestProcessor() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile toRemove = session.get();
        if (toRemove != null) {
            LOG.warn("Removing flow file");
            session.remove(toRemove);
        }

        FlowFile flowFile = session.create();
        final Random random = new Random();
        final byte[] data = new byte[4096];
        random.nextBytes(data);

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                out.write(data);
            }
        });

        LOG.info("{} transferring {} to success", new Object[]{this, flowFile});
        session.transfer(flowFile, REL_SUCCESS);
        session.commit();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
    }

}
