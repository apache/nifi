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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.stream.io.StreamUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class TestMockProcessSession {

    @Test
    public void testReadWithoutCloseThrowsExceptionOnCommit() throws IOException {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, true, new MockStateManager(processor), true);
        FlowFile flowFile = session.createFlowFile("hello, world".getBytes());
        final InputStream in = session.read(flowFile);
        final byte[] buffer = new byte[12];
        StreamUtils.fillBuffer(in, buffer);

        assertEquals("hello, world", new String(buffer));

        try {
            session.commit();
            fail("Was able to commit session without closing InputStream");
        } catch (final FlowFileHandlingException | IllegalStateException e) {
            System.out.println(e.toString());
        }
    }

    @Test
    public void testReadWithoutCloseThrowsExceptionOnCommitAsync() throws IOException {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        FlowFile flowFile = session.createFlowFile("hello, world".getBytes());
        final InputStream in = session.read(flowFile);
        final byte[] buffer = new byte[12];
        StreamUtils.fillBuffer(in, buffer);

        assertEquals("hello, world", new String(buffer));

        try {
            session.commitAsync();
            fail("Was able to commit session without closing InputStream");
        } catch (final FlowFileHandlingException | IllegalStateException e) {
            System.out.println(e.toString());
        }
    }

    @Test
    public void testTransferUnknownRelationship() {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        FlowFile ff1 = session.createFlowFile("hello, world".getBytes());
        final Relationship fakeRel = new Relationship.Builder().name("FAKE").build();
        try {
            session.transfer(ff1, fakeRel);
            fail("Should have thrown IllegalArgumentException");
        } catch (final IllegalArgumentException ie) {

        }
        try {
            session.transfer(Collections.singleton(ff1), fakeRel);
            fail("Should have thrown IllegalArgumentException");
        } catch (final IllegalArgumentException ie) {

        }

    }

    @Test
    public void testRejectTransferNewlyCreatedFileToSelf() {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        final FlowFile ff1 = session.createFlowFile("hello, world".getBytes());
        // this should throw an exception because we shouldn't allow a newly created flowfile to get routed back to self
        assertThrows(IllegalArgumentException.class, () -> session.transfer(ff1));
    }

    @Test
    public void testKeepPenalizedStatusAfterPuttingAttribute(){
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        FlowFile ff1 = session.createFlowFile("hello, world".getBytes());
        ff1 = session.penalize(ff1);
        assertTrue(ff1.isPenalized());
        ff1 = session.putAttribute(ff1, "hello", "world");
        // adding attribute to flow file should not override the original penalized status
        assertTrue(ff1.isPenalized());
    }

    @Test
    public void testUnpenalizeFlowFile() {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        FlowFile ff1 = session.createFlowFile("hello, world".getBytes());
        ff1 = session.penalize(ff1);
        assertTrue(ff1.isPenalized());
        ff1 = session.unpenalize(ff1);
        assertFalse(ff1.isPenalized());
    }

    @Test
    public void testRollbackWithCreatedFlowFile() {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        final FlowFile ff1 = session.createFlowFile("hello, world".getBytes());
        session.transfer(ff1, PoorlyBehavedProcessor.REL_FAILURE);
        session.rollback();
        session.assertQueueEmpty();
    }

    @Test
    public void testRollbackWithClonedFlowFile() {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        final FlowFile ff1 = session.createFlowFile("hello, world".getBytes());
        session.clone(ff1);
        session.transfer(ff1, PoorlyBehavedProcessor.REL_FAILURE);
        session.rollback();
        session.assertQueueEmpty();
    }

    @Test
    public void testRollbackWithMigratedFlowFile() {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        final MockProcessSession newSession = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        final FlowFile ff1 = session.createFlowFile("hello, world".getBytes());
        session.migrate(newSession);
        newSession.transfer(ff1, PoorlyBehavedProcessor.REL_FAILURE);
        newSession.rollback();
        session.assertQueueEmpty();
        newSession.assertQueueEmpty();
    }

    @Test
    public void testAttributePreservedAfterWrite() throws IOException {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        FlowFile ff1 = session.createFlowFile("hello, world".getBytes());
        session.putAttribute(ff1, "key1", "val1");
        session.write(ff1).close();
        session.transfer(ff1, PoorlyBehavedProcessor.REL_FAILURE);
        session.commitAsync();
        List<MockFlowFile> output = session.getFlowFilesForRelationship(PoorlyBehavedProcessor.REL_FAILURE);
        assertEquals(1, output.size());
        output.get(0).assertAttributeEquals("key1", "val1");
    }

    @Test
    void testAttributeUUIDNotRemovable() {
        final Processor processor = new PoorlyBehavedProcessor();
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0L)), processor, new MockStateManager(processor));
        FlowFile ff1 = session.createFlowFile("removeAttribute(attrName)".getBytes());
        FlowFile ff2 = session.createFlowFile("removeAllAttributes(attrNames)".getBytes());
        FlowFile ff3 = session.createFlowFile("removeAllAttributes(keyPattern)".getBytes());

        String attrName = CoreAttributes.UUID.key();
        session.removeAttribute(ff1, attrName);
        session.removeAllAttributes(ff2, new HashSet<>(Collections.singletonList(attrName)));
        session.removeAllAttributes(ff3, Pattern.compile(Pattern.quote(attrName)));

        session.transfer(Arrays.asList(ff1, ff2, ff3), PoorlyBehavedProcessor.REL_FAILURE);
        session.commitAsync();
        List<MockFlowFile> output = session.getFlowFilesForRelationship(PoorlyBehavedProcessor.REL_FAILURE);
        assertEquals(3, output.size());
        output.get(0).assertAttributeEquals(attrName, ff1.getAttribute(attrName));
        output.get(1).assertAttributeEquals(attrName, ff2.getAttribute(attrName));
        output.get(2).assertAttributeEquals(attrName, ff3.getAttribute(attrName));
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
