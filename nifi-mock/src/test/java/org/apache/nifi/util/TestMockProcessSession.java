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

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestMockProcessSession {

    private final Processor processor = new TestProcessor();
    private final SharedSessionState sharedState = new SharedSessionState(processor, new AtomicLong(0L));
    private final MockStateManager stateManager = new MockStateManager(processor);
    private final MockProcessSession session = new MockProcessSession(sharedState, processor, stateManager);

    private final Processor statefulProcessor = new StatefulTestProcessor();
    private final SharedSessionState sharedStateOfStatefulProcessor = new SharedSessionState(statefulProcessor, new AtomicLong(0L));
    private final MockStateManager stateManagerOfStatefulProcessor = new MockStateManager(statefulProcessor);
    private final MockProcessSession sessionOfStatefulProcessor = new MockProcessSession(sharedStateOfStatefulProcessor, statefulProcessor, stateManagerOfStatefulProcessor);

    @Nested
    class RegardingActiveReads {
        @Test
        void cannotTransferFlowFileThatIsReadActively() throws IOException {
            MockFlowFile flowFile = session.createFlowFile("hello, world".getBytes());
            readWithoutClosingInputStream(session, flowFile);

            assertThrows(IllegalStateException.class, () -> {
                session.transfer(flowFile, TestProcessor.REL_KNOWN);
            }, "Was able to transfer FlowFile without closing InputStream");
        }

        @Test
        void cannotRemoveFlowFileThatIsReadActively() throws IOException {
            MockFlowFile flowFile = session.createFlowFile("hello, world".getBytes());
            readWithoutClosingInputStream(session, flowFile);

            assertThrows(IllegalStateException.class, () -> {
                session.remove(flowFile);
            }, "Was able to remove FlowFile without closing InputStream");
        }

        @Test
        void cannotMergeWithFlowFileThatIsReadActively() throws IOException {
            MockFlowFile offendingFlowFile = session.createFlowFile("hello, world".getBytes());
            readWithoutClosingInputStream(session, offendingFlowFile);
            MockFlowFile otherFlowFile = session.createFlowFile("Hola mundo".getBytes());
            MockFlowFile destinationFlowFile = session.create();

            assertThrows(IllegalStateException.class, () -> {
                session.merge(Set.of(offendingFlowFile, otherFlowFile), destinationFlowFile);
            }, "Was able to merge FlowFile without closing InputStream");
        }

        @Test
        void cannotMigrateFlowFileThatIsReadActively() throws IOException {
            final MockProcessSession targetSession = createMockProcessSession();

            MockFlowFile offendingFlowFile = session.createFlowFile("hello, world".getBytes());
            readWithoutClosingInputStream(session, offendingFlowFile);

            assertThrows(IllegalStateException.class, () -> {
                session.migrate(targetSession);
            }, "Was able to merge FlowFile without closing InputStream");
        }

        private static void readWithoutClosingInputStream(MockProcessSession session, MockFlowFile flowFile) throws IOException {
            String expectedContent = flowFile.getContent();

            @SuppressWarnings("resource") final InputStream in = session.read(flowFile);
            final byte[] bytes = in.readAllBytes();
            assertEquals(expectedContent, new String(bytes));
        }
    }

    @Nested
    class RegardingActiveWrites {
        @Test
        void cannotTransferFlowFileThatIsWrittenActively() throws IOException {
            MockFlowFile flowFile = session.create();
            writeWithoutClosingOutputStream(session, flowFile);

            assertThrows(IllegalStateException.class, () -> {
                session.transfer(flowFile, TestProcessor.REL_KNOWN);
            }, "Was able to transfer FlowFile without closing OutputStream");
        }

        @Test
        void cannotRemoveFlowFileThatIsWrittenActively() throws IOException {
            MockFlowFile flowFile = session.create();
            writeWithoutClosingOutputStream(session, flowFile);

            assertThrows(IllegalStateException.class, () -> {
                session.remove(flowFile);
            }, "Was able to remove FlowFile without closing OutputStream");
        }

        @Test
        void cannotMergeWithFlowFileThatIsWrittenActively() throws IOException {
            MockFlowFile offendingFlowFile = session.createFlowFile("hello, world".getBytes());
            writeWithoutClosingOutputStream(session, offendingFlowFile);
            MockFlowFile otherFlowFile = session.createFlowFile("Hola mundo".getBytes());
            MockFlowFile destinationFlowFile = session.create();

            assertThrows(IllegalStateException.class, () -> {
                session.merge(Set.of(offendingFlowFile, otherFlowFile), destinationFlowFile);
            }, "Was able to merge FlowFile without closing OutputStream");
        }

        @Test
        void cannotMigrateFlowFileThatIsWrittenActively() throws IOException {
            final MockProcessSession targetSession = createMockProcessSession();

            MockFlowFile offendingFlowFile = session.create();
            writeWithoutClosingOutputStream(session, offendingFlowFile);

            assertThrows(IllegalStateException.class, () -> {
                session.migrate(targetSession);
            }, "Was able to merge FlowFile without closing OutputStream");
        }

        private static void writeWithoutClosingOutputStream(MockProcessSession session, MockFlowFile flowFile) throws IOException {
            @SuppressWarnings("resource") final OutputStream outputStream = session.write(flowFile);
            outputStream.write("some content".getBytes());
        }
    }

    @Nested
    class RegardingUnaccountedFlowFiles {

        @Test
        void cannotCommitWithUnaccountedCreatedFlowFile() {
            session.create(); // unaccounted for

            assertThrows(FlowFileHandlingException.class, session::commitAsync, "Was able to commit with unaccounted newly created FlowFile");
        }

        @Test
        void cannotCommitWithUnaccountedClonedFlowFile() {
            MockFlowFile flowFile = session.create();
            session.clone(flowFile); // unaccounted for
            session.transfer(flowFile, TestProcessor.REL_KNOWN);

            assertThrows(FlowFileHandlingException.class, session::commitAsync, "Was able to commit with unaccounted cloned FlowFile");
        }

        @Test
        void cannotCommitWithUnaccountedMigratedFlowFile() {
            MockProcessSession targetSession = createMockProcessSession();
            session.create();
            session.migrate(targetSession);

            assertThrows(FlowFileHandlingException.class, targetSession::commitAsync, "Was able to commit with unaccounted FlowFile that immigrated");
            assertDoesNotThrow(() -> session.commitAsync(), "Was not able to commit session that migrated its FlowFiles");
        }
    }

    @Nested
    class RegardingTransfer {

        @Test
        void canTransferSingleFlowFileToKnownRelationship() {
            MockFlowFile flowFile = session.create();

            assertDoesNotThrow(() -> session.transfer(flowFile, TestProcessor.REL_KNOWN));

            session.commitAsync();
            session.assertAllFlowFilesTransferred(TestProcessor.REL_KNOWN, 1);
        }

        @Test
        void canTransferMultipleFlowFilesToKnownRelationship() {
            Collection<FlowFile> flowFiles = Set.of(session.create(), session.create(), session.create());

            assertDoesNotThrow(() -> session.transfer(flowFiles, TestProcessor.REL_KNOWN));

            session.commitAsync();
            session.assertAllFlowFilesTransferred(TestProcessor.REL_KNOWN, flowFiles.size());
        }

        @Test
        void canTransferSingleFlowFileToSelfRelationship() {
            enqueueFlowFile();
            MockFlowFile flowFile = session.get();

            assertDoesNotThrow(() -> session.transfer(flowFile));

            session.commitAsync();
            session.assertTransferCount(TestProcessor.REL_KNOWN, 0);
            assertObjectCountInQueue(session, 1);
        }

        @Test
        void canTransferMultipleFlowFilesToSelfRelationship() {
            enqueueFlowFile();
            enqueueFlowFile();
            enqueueFlowFile();
            Collection<FlowFile> flowFiles = session.get(3);

            assertDoesNotThrow(() -> session.transfer(flowFiles));

            session.commitAsync();
            session.assertTransferCount(TestProcessor.REL_KNOWN, 0);
            assertObjectCountInQueue(session, flowFiles.size());
        }

        @Test
        void cannotTransferToUnknownRelationship() {
            MockFlowFile flowFile = session.create();
            Collection<FlowFile> flowFiles = Set.of(session.create(), session.create(), session.create());
            final Relationship unknownRelationship = new Relationship.Builder().name("unknown").build();

            assertThrows(IllegalArgumentException.class, () -> session.transfer(flowFile, unknownRelationship), "Was able to transfer to unknown relationship");
            assertThrows(IllegalArgumentException.class, () -> session.transfer(flowFiles, unknownRelationship), "Was able to transfer to unknown relationship");
        }

        @Test
        void cannotTransferNewlyCreatedFlowFilesToSelfRelationship() {
            MockFlowFile flowFile = session.create();
            Collection<FlowFile> flowFiles = Set.of(session.create(), session.create(), session.create());

            assertThrows(IllegalArgumentException.class, () -> session.transfer(flowFile), "Was able to transfer newly created FlowFile to self relationship");
            assertThrows(IllegalArgumentException.class, () -> session.transfer(flowFiles), "Was able to transfer newly created FlowFiles to self relationship");
        }

        @Test
        void cannotTransferClonedFlowFilesToSelfRelationship() {
            MockFlowFile flowFile = session.create();
            MockFlowFile clonedFlowFile = session.clone(flowFile);
            Collection<FlowFile> clonedFlowFiles = Set.of(session.clone(flowFile), session.clone(flowFile));

            assertThrows(IllegalArgumentException.class, () -> session.transfer(clonedFlowFile), "Was able to transfer cloned FlowFile to self relationship");
            assertThrows(IllegalArgumentException.class, () -> session.transfer(clonedFlowFiles), "Was able to transfer cloned FlowFiles to self relationship");
        }
    }

    @Nested
    class RegardingPenalizedState {
        @Test
        void keepsPenalizedStatusAfterAttributeWrite() {
            MockFlowFile flowFile = session.create();

            flowFile = session.penalize(flowFile);
            flowFile = session.putAttribute(flowFile, "Foo", "Bar");

            session.transfer(flowFile, TestProcessor.REL_KNOWN);
            session.commitAsync();
            assertTrue(() -> getSingleFlowFileInRelationship().isPenalized(), "FlowFile was not penalized");
        }

        @Test
        void keepsPenalizedStatusAfterContentWrite() {
            MockFlowFile flowFile = session.create();

            flowFile = session.penalize(flowFile);
            flowFile = session.write(flowFile, (outputStream) -> outputStream.write("test content".getBytes()));

            session.transfer(flowFile, TestProcessor.REL_KNOWN);
            session.commitAsync();
            assertTrue(() -> getSingleFlowFileInRelationship().isPenalized(), "FlowFile was not penalized");
        }

        @Test
        void penalizedStatusCanBeReset() {
            MockFlowFile flowFile = session.create();

            flowFile = session.penalize(flowFile);
            flowFile = session.unpenalize(flowFile);

            session.transfer(flowFile, TestProcessor.REL_KNOWN);
            session.commitAsync();
            assertFalse(() -> getSingleFlowFileInRelationship().isPenalized(), "FlowFile was penalized");
        }
    }

    @Nested
    class RegardingAttributes {

        private final String UUID_ATTRIBUTE_NAME = CoreAttributes.UUID.key();

        @Test
        void canWriteAttribute() {
            MockFlowFile flowFile = session.create();

            flowFile = session.putAttribute(flowFile, "Hello", "world");

            session.transfer(flowFile, TestProcessor.REL_KNOWN);
            session.commitAsync();
            getSingleFlowFileInRelationship().assertAttributeEquals("Hello", "world");
        }

        @Test
        void canWriteAttributes() {
            MockFlowFile flowFile = session.create();

            flowFile = session.putAllAttributes(flowFile, Map.of("Hello", "world", "Hola", "mundo"));

            session.transfer(flowFile, TestProcessor.REL_KNOWN);
            session.commitAsync();
            MockFlowFile resultFlowFile = getSingleFlowFileInRelationship();
            resultFlowFile.assertAttributeEquals("Hello", "world");
            resultFlowFile.assertAttributeEquals("Hola", "mundo");
        }

        @Test
        void writtenAttributesAreNotAffectedByContentWrite() {
            MockFlowFile flowFile = session.create();

            flowFile = session.putAttribute(flowFile, "Hello", "world");
            flowFile = session.write(flowFile, (outputStream) -> outputStream.write("test content".getBytes()));
            flowFile = session.putAttribute(flowFile, "Hola", "mundo");

            session.transfer(flowFile, TestProcessor.REL_KNOWN);
            session.commitAsync();
            MockFlowFile resultFlowFile = getSingleFlowFileInRelationship();
            resultFlowFile.assertAttributeEquals("Hello", "world");
            resultFlowFile.assertAttributeEquals("Hola", "mundo");
        }

        @Test
        void cannotModifyUUID() {
            MockFlowFile flowFile = session.create();
            String expectedUuid = flowFile.getAttribute(UUID_ATTRIBUTE_NAME);

            assertThrows(AssertionError.class, () -> session.putAttribute(flowFile, UUID_ATTRIBUTE_NAME, "put single"));
            session.putAllAttributes(flowFile, Map.of(UUID_ATTRIBUTE_NAME, "put multiple", "foo", "bar"));

            session.transfer(flowFile, TestProcessor.REL_KNOWN);
            session.commitAsync();
            assertEquals(expectedUuid, getSingleFlowFileInRelationship().getAttribute(UUID_ATTRIBUTE_NAME));
        }

        @Test
        void cannotRemoveUUID() {
            MockFlowFile flowFile = session.create();
            String expectedUuid = flowFile.getAttribute(UUID_ATTRIBUTE_NAME);

            flowFile = session.removeAttribute(flowFile, UUID_ATTRIBUTE_NAME);
            flowFile = session.removeAllAttributes(flowFile, Set.of(UUID_ATTRIBUTE_NAME));
            flowFile = session.removeAllAttributes(flowFile, Pattern.compile(Pattern.quote(UUID_ATTRIBUTE_NAME)));

            session.transfer(flowFile, TestProcessor.REL_KNOWN);
            session.commitAsync();
            assertEquals(expectedUuid, getSingleFlowFileInRelationship().getAttribute(UUID_ATTRIBUTE_NAME));
        }
    }

    @Nested
    class RegardingRollbacks {

        @Test
        void flowFilesArePutBackToQueueOnRollback() {
            enqueueFlowFile();
            MockFlowFile flowFile = session.get();
            session.transfer(flowFile, TestProcessor.REL_KNOWN);

            session.rollback();

            assertObjectCountInQueue(session, 1);
            session.assertTransferCount(TestProcessor.REL_KNOWN, 0);
        }

        @Test
        void attributeChangesAreResetOnRollback() {
            enqueueFlowFile();
            MockFlowFile flowFile = session.get();
            session.putAttribute(flowFile, "attribute", "changed");

            session.rollback();

            assertObjectCountInQueue(session, 1);
            session.assertTransferCount(TestProcessor.REL_KNOWN, 0);
            assertObjectCountInQueue(session, 1);
            MockFlowFile resultFlowFile = sharedState.getFlowFileQueue().poll();
            resultFlowFile.assertAttributeNotExists("attribute");
        }

        @Test
        void contentChangesAreResetOnRollback() {
            enqueueFlowFile();
            MockFlowFile flowFile = session.get();
            String expectedContent = flowFile.getContent();
            session.write(flowFile, (outputStream) -> outputStream.write("changed content".getBytes()));

            session.rollback();

            assertObjectCountInQueue(session, 1);
            session.assertTransferCount(TestProcessor.REL_KNOWN, 0);
            assertObjectCountInQueue(session, 1);
            MockFlowFile resultFlowFile = sharedState.getFlowFileQueue().poll();
            resultFlowFile.assertContentEquals(expectedContent);
        }

        @Disabled("The current implementation of MockProcessSession does not express this behavior defined in the interface")
        @Test
        void stateChangesAreResetOnRollback() throws IOException {
            sessionOfStatefulProcessor.setState(Map.of("attribute", "local"), Scope.LOCAL);
            sessionOfStatefulProcessor.setState(Map.of("attribute", "cluster"), Scope.CLUSTER);

            sessionOfStatefulProcessor.rollback();

            stateManagerOfStatefulProcessor.assertStateNotSet();
            stateManagerOfStatefulProcessor.assertStateEquals(Map.of(), Scope.LOCAL);
            stateManagerOfStatefulProcessor.assertStateEquals(Map.of(), Scope.CLUSTER);
        }

        @Test
        void newlyCreatedFlowFilesAreRemovedOnRollback() {
            MockFlowFile flowFile = session.create();
            session.transfer(flowFile, TestProcessor.REL_KNOWN);

            session.rollback();

            session.assertQueueEmpty();
            session.assertTransferCount(TestProcessor.REL_KNOWN, 0);
        }

        @Test
        void clonedFlowFilesAreRemovedOnRollback() {
            enqueueFlowFile();
            MockFlowFile flowFile = session.get();
            MockFlowFile clonedFlowFile = session.clone(flowFile);
            session.transfer(flowFile, TestProcessor.REL_KNOWN);
            session.transfer(clonedFlowFile, TestProcessor.REL_KNOWN);

            session.rollback();

            assertObjectCountInQueue(session, 1);
            session.assertTransferCount(TestProcessor.REL_KNOWN, 0);
        }

        @Test
        void migrateFlowFileIsPutToQueueOfNewOwnerOnRollback() {
            final MockProcessSession targetSession = createMockProcessSession();
            enqueueFlowFile();
            MockFlowFile flowFile = session.get();
            session.migrate(targetSession);
            targetSession.transfer(flowFile, TestProcessor.REL_KNOWN);

            targetSession.rollback();

            session.assertQueueEmpty();
            session.assertTransferCount(TestProcessor.REL_KNOWN, 0);
            assertObjectCountInQueue(targetSession, 1);
            targetSession.assertTransferCount(TestProcessor.REL_KNOWN, 0);
        }
    }

    @Nested
    class RegardingState {

        @Test
        void cannotClearLocalStateUnlessDeclaredStateful() {
            assertThrows(AssertionError.class, () -> {
                session.clearState(Scope.LOCAL);
            }, "Was able to clear local state without declaring the being stateful");
            stateManager.assertStateNotSet();
        }

        @Test
        void cannotClearClusterStateUnlessDeclaredStateful() {
            assertThrows(AssertionError.class, () -> {
                session.clearState(Scope.CLUSTER);
            }, "Was able to clear cluster state without declaring the being stateful");
            stateManager.assertStateNotSet();
        }

        @Test
        void canClearLocalStateWhenDeclaredStateful() throws IOException {
            stateManagerOfStatefulProcessor.setState(Map.of("existing", "value"), Scope.LOCAL);

            sessionOfStatefulProcessor.clearState(Scope.LOCAL);

            sessionOfStatefulProcessor.commitAsync();
            stateManagerOfStatefulProcessor.assertStateSet(Scope.LOCAL);
            stateManagerOfStatefulProcessor.assertStateEquals(Map.of(), Scope.LOCAL);
        }

        @Test
        void canClearClusterStateWhenDeclaredStateful() throws IOException {
            stateManagerOfStatefulProcessor.setState(Map.of("existing", "value"), Scope.LOCAL);

            sessionOfStatefulProcessor.clearState(Scope.CLUSTER);

            sessionOfStatefulProcessor.commitAsync();
            stateManagerOfStatefulProcessor.assertStateSet(Scope.CLUSTER);
            stateManagerOfStatefulProcessor.assertStateEquals(Map.of(), Scope.CLUSTER);
        }

        @Test
        void cannotGetLocalStateUnlessDeclaredStateful() {
            assertThrows(AssertionError.class, () -> {
                session.getState(Scope.LOCAL);
            }, "Was able to get local state without declaring the being stateful");
            stateManager.assertStateNotSet();
        }

        @Test
        void cannotGetClusterStateUnlessDeclaredStateful() {
            assertThrows(AssertionError.class, () -> {
                session.getState(Scope.CLUSTER);
            }, "Was able to get cluster state without declaring the being stateful");
            stateManager.assertStateNotSet();
        }

        @Test
        void canGetLocalStateWhenDeclaredStateful() throws IOException {
            Map<String, String> expectedState = Map.of("key", "value");
            stateManagerOfStatefulProcessor.setState(expectedState, Scope.LOCAL);

            StateMap result = sessionOfStatefulProcessor.getState(Scope.LOCAL);

            assertEquals(expectedState, result.toMap());
        }

        @Test
        void canGetClusterStateWhenDeclaredStateful() throws IOException {
            Map<String, String> expectedState = Map.of("key", "value");
            stateManagerOfStatefulProcessor.setState(expectedState, Scope.CLUSTER);

            StateMap result = sessionOfStatefulProcessor.getState(Scope.CLUSTER);

            assertEquals(expectedState, result.toMap());
        }

        @Test
        void cannotSetLocalStateUnlessDeclaredStateful() {
            assertThrows(AssertionError.class, () -> {
                session.setState(Map.of("key", "value"), Scope.LOCAL);
            }, "Was able to set local state without declaring the being stateful");
            stateManager.assertStateNotSet();
        }

        @Test
        void cannotSetClusterStateUnlessDeclaredStateful() {
            assertThrows(AssertionError.class, () -> {
                session.setState(Map.of("key", "value"), Scope.CLUSTER);
            }, "Was able to set cluster state without declaring the being stateful");
            stateManager.assertStateNotSet();
        }

        @Test
        void canSetLocalStateWhenDeclaredStateful() throws IOException {
            Map<String, String> expectedState = Map.of("key", "value");
            sessionOfStatefulProcessor.setState(expectedState, Scope.LOCAL);

            sessionOfStatefulProcessor.commitAsync();
            assertEquals(expectedState, sessionOfStatefulProcessor.getState(Scope.LOCAL).toMap());
        }

        @Test
        void canSetClusterStateWhenDeclaredStateful() throws IOException {
            Map<String, String> expectedState = Map.of("key", "value");
            sessionOfStatefulProcessor.setState(expectedState, Scope.CLUSTER);

            sessionOfStatefulProcessor.commitAsync();
            assertEquals(expectedState, sessionOfStatefulProcessor.getState(Scope.CLUSTER).toMap());
        }
    }

    private MockProcessSession createMockProcessSession() {
        return createMockProcessSession(new TestProcessor());
    }

    private static MockProcessSession createMockProcessSession(Processor processor) {
        final SharedSessionState sharedState = new SharedSessionState(processor, new AtomicLong(0L));
        return new MockProcessSession(sharedState, processor, new MockStateManager(processor));
    }

    private void enqueueFlowFile() {
        MockFlowFile flowFile = new MockFlowFile(sharedState.nextFlowFileId());
        flowFile.setData("test content".getBytes());

        sharedState.getFlowFileQueue().offer(flowFile);
    }

    private MockFlowFile getSingleFlowFileInRelationship() {
        List<MockFlowFile> flowFiles = session.getFlowFilesForRelationship(TestProcessor.REL_KNOWN);
        assertEquals(1, flowFiles.size());

        return flowFiles.getFirst();
    }

    private void assertObjectCountInQueue(MockProcessSession processSession, int expectedObjectCount) {
        int actualObjectCount = processSession.getQueueSize().getObjectCount();
        assertEquals(expectedObjectCount, actualObjectCount, "Queue had " + actualObjectCount + " FlowFile(s) but expected " + expectedObjectCount);
    }

    private static class TestProcessor extends AbstractProcessor {

        private static final Relationship REL_KNOWN = new Relationship.Builder().name("known").build();
        private static final Set<Relationship> relationships = Set.of(REL_KNOWN);

        @Override
        public Set<Relationship> getRelationships() {
            return relationships;
        }

        @Override
        public void onTrigger(final ProcessContext ctx, final ProcessSession session) throws ProcessException {
            fail("onTrigger of TestProcessor is not designed to be invoked");
        }
    }

    @Stateful(description = "scopes for tests", scopes = {Scope.LOCAL, Scope.CLUSTER})
    private static class StatefulTestProcessor extends TestProcessor {}
}
