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
package org.apache.nifi.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.StreamUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestMockProcessSession {

    @Test
    public void testReadWithoutCloseThrowsExceptionOnCommit() throws IOException {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor);
        FlowFile flowFile = session.createFlowFile("hello, world".getBytes());
        final InputStream in = session.read(flowFile);
        final byte[] buffer = new byte[12];
        StreamUtils.fillBuffer(in, buffer);

        assertEquals("hello, world", new String(buffer));

        session.remove(flowFile);

        try {
            session.commit();
            Assert.fail("Was able to commit session without closing InputStream");
        } catch (final FlowFileHandlingException ffhe) {
            System.out.println(ffhe.toString());
        }
    }

    @Test
    public void testTransferUnknownRelationship() {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor);
        FlowFile ff1 = session.createFlowFile("hello, world".getBytes());
        final Relationship fakeRel = new Relationship.Builder().name("FAKE").build();
        try {
            session.transfer(ff1, fakeRel);
            Assert.fail("Should have thrown IllegalArgumentException");
        } catch (final IllegalArgumentException ie) {

        }
        try {
            session.transfer(Collections.singleton(ff1), fakeRel);
            Assert.fail("Should have thrown IllegalArgumentException");
        } catch (final IllegalArgumentException ie) {

        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectTransferNewlyCreatedFileToSelf() {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor);
        final FlowFile ff1 = session.createFlowFile("hello, world".getBytes());
        // this should throw an exception because we shouldn't allow a newly created flowfile to get routed back to self
        session.transfer(ff1);
    }

    protected static class PoorlyBehavedProcessor extends AbstractProcessor {

        private static final Relationship REL_FAILURE = new Relationship.Builder()
                .name("failure")
                .build();

        private final Set<Relationship> relationships = Collections.singleton(REL_FAILURE);

        @Override
        public Set<Relationship> getRelationships() {
            return relationships;
        }

        @Override
        public void onTrigger(final ProcessContext ctx, final ProcessSession session) throws ProcessException {
            final FlowFile file = session.create();
            session.penalize(file);
            session.transfer(file, REL_FAILURE);
        }

    }
}
