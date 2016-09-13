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
package org.apache.nifi.processors.standard;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestDebugFlow {

    private DebugFlow debugFlow;
    private TestRunner runner;
    private ProcessSession session;

    private final Map<Integer, String> contents = new HashMap<>();
    private final Map<Integer, Map<String, String>> attribs = new HashMap<>();
    private Map<String, String> namesToContent = new HashMap<>();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        for (int n = 0; n < 6; n++) {
            String filename = "testFile" + (n + 1) + ".txt";
            String content = "Hello World " + (n + 1) + "!";
            contents.put(n, content);
            attribs.put(n, new HashMap<String, String>());
            attribs.get(n).put(CoreAttributes.FILENAME.key(), filename);
            attribs.get(n).put(CoreAttributes.UUID.key(), "TESTING-FILE-" + (n + 1) + "-TESTING");
            namesToContent.put(filename, content);
        }

        debugFlow = new DebugFlow();
        runner = TestRunners.newTestRunner(debugFlow);
        session = runner.getProcessSessionFactory().createSession();

        runner.setProperty(DebugFlow.FF_SUCCESS_ITERATIONS, "0");
        runner.setProperty(DebugFlow.FF_FAILURE_ITERATIONS, "0");
        runner.setProperty(DebugFlow.FF_ROLLBACK_ITERATIONS, "0");
        runner.setProperty(DebugFlow.FF_ROLLBACK_YIELD_ITERATIONS, "0");
        runner.setProperty(DebugFlow.FF_ROLLBACK_PENALTY_ITERATIONS, "0");
        runner.setProperty(DebugFlow.FF_EXCEPTION_ITERATIONS, "0");

        runner.setProperty(DebugFlow.NO_FF_SKIP_ITERATIONS, "0");
        runner.setProperty(DebugFlow.NO_FF_EXCEPTION_ITERATIONS, "0");
        runner.setProperty(DebugFlow.NO_FF_YIELD_ITERATIONS, "0");
    }

    private boolean isInContents(byte[] content) {
        return contents.containsValue(new String(content));
    }

    @Test
    public void testFlowFileSuccess() {
        runner.setProperty(DebugFlow.FF_SUCCESS_ITERATIONS, "1");
        runner.assertValid();

        for (int n = 0; n < 6; n++) {
            runner.enqueue(contents.get(n).getBytes(), attribs.get(n));
        }

        runner.run(7);
        runner.assertTransferCount(DebugFlow.REL_SUCCESS, 6);
        runner.assertTransferCount(DebugFlow.REL_FAILURE, 0);

        assertTrue(isInContents(runner.getFlowFilesForRelationship(DebugFlow.REL_SUCCESS).get(0).toByteArray()));
        assertTrue(isInContents(runner.getFlowFilesForRelationship(DebugFlow.REL_SUCCESS).get(1).toByteArray()));
        assertTrue(isInContents(runner.getFlowFilesForRelationship(DebugFlow.REL_SUCCESS).get(2).toByteArray()));
        assertTrue(isInContents(runner.getFlowFilesForRelationship(DebugFlow.REL_SUCCESS).get(3).toByteArray()));
        assertTrue(isInContents(runner.getFlowFilesForRelationship(DebugFlow.REL_SUCCESS).get(4).toByteArray()));
        assertTrue(isInContents(runner.getFlowFilesForRelationship(DebugFlow.REL_SUCCESS).get(5).toByteArray()));
    }

    @Test
    public void testFlowFileFailure() {
        runner.setProperty(DebugFlow.FF_FAILURE_ITERATIONS, "1");
        runner.assertValid();

        for (int n = 0; n < 6; n++) {
            runner.enqueue(contents.get(n).getBytes(), attribs.get(n));
        }

        runner.run(7);
        runner.assertTransferCount(DebugFlow.REL_SUCCESS, 0);
        runner.assertTransferCount(DebugFlow.REL_FAILURE, 6);

        runner.getFlowFilesForRelationship(DebugFlow.REL_FAILURE).get(0).assertContentEquals(contents.get(0));
        runner.getFlowFilesForRelationship(DebugFlow.REL_FAILURE).get(1).assertContentEquals(contents.get(1));
        runner.getFlowFilesForRelationship(DebugFlow.REL_FAILURE).get(2).assertContentEquals(contents.get(2));
        runner.getFlowFilesForRelationship(DebugFlow.REL_FAILURE).get(3).assertContentEquals(contents.get(3));
        runner.getFlowFilesForRelationship(DebugFlow.REL_FAILURE).get(4).assertContentEquals(contents.get(4));
        runner.getFlowFilesForRelationship(DebugFlow.REL_FAILURE).get(5).assertContentEquals(contents.get(5));
    }

    @Test
    public void testFlowFileSuccessAndFailure() {
        runner.setProperty(DebugFlow.FF_SUCCESS_ITERATIONS, "1");
        runner.setProperty(DebugFlow.FF_FAILURE_ITERATIONS, "1");
        runner.assertValid();

        for (int n = 0; n < 6; n++) {
            runner.enqueue(contents.get(n).getBytes(), attribs.get(n));
        }

        runner.run(7);
        runner.assertTransferCount(DebugFlow.REL_SUCCESS, 3);
        runner.assertTransferCount(DebugFlow.REL_FAILURE, 3);

        runner.getFlowFilesForRelationship(DebugFlow.REL_SUCCESS).get(0).assertContentEquals(contents.get(0));
        runner.getFlowFilesForRelationship(DebugFlow.REL_FAILURE).get(0).assertContentEquals(contents.get(1));
        runner.getFlowFilesForRelationship(DebugFlow.REL_SUCCESS).get(1).assertContentEquals(contents.get(2));
        runner.getFlowFilesForRelationship(DebugFlow.REL_FAILURE).get(1).assertContentEquals(contents.get(3));
        runner.getFlowFilesForRelationship(DebugFlow.REL_SUCCESS).get(2).assertContentEquals(contents.get(4));
        runner.getFlowFilesForRelationship(DebugFlow.REL_FAILURE).get(2).assertContentEquals(contents.get(5));
    }

    @Test
    public void testFlowFileRollback() throws IOException {
        runner.setProperty(DebugFlow.FF_ROLLBACK_ITERATIONS, "1");
        runner.assertValid();

        for (int n = 0; n < 6; n++) {
            runner.enqueue(contents.get(n).getBytes(), attribs.get(n));
        }

        runner.run(7);
        runner.assertTransferCount(DebugFlow.REL_SUCCESS, 0);
        runner.assertTransferCount(DebugFlow.REL_FAILURE, 0);

        runner.assertQueueNotEmpty();
        assertEquals(6, runner.getQueueSize().getObjectCount());

        MockFlowFile ff1 = (MockFlowFile) session.get();
        assertNotNull(ff1);
        assertEquals(namesToContent.get(ff1.getAttribute(CoreAttributes.FILENAME.key())), new String(ff1.toByteArray()));
        session.rollback();
    }

    @Test
    public void testFlowFileRollbackYield() {
        runner.setProperty(DebugFlow.FF_ROLLBACK_YIELD_ITERATIONS, "1");
        runner.assertValid();

        for (int n = 0; n < 6; n++) {
            runner.enqueue(contents.get(n).getBytes(), attribs.get(n));
        }

        runner.run(7);
        runner.assertTransferCount(DebugFlow.REL_SUCCESS, 0);
        runner.assertTransferCount(DebugFlow.REL_FAILURE, 0);

        runner.assertQueueNotEmpty();
        assertEquals(6, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void testFlowFileRollbackPenalty() {
        runner.setProperty(DebugFlow.FF_ROLLBACK_PENALTY_ITERATIONS, "1");
        runner.assertValid();

        for (int n = 0; n < 6; n++) {
            runner.enqueue(contents.get(n).getBytes(), attribs.get(n));
        }

        runner.run(7);
        runner.assertTransferCount(DebugFlow.REL_SUCCESS, 0);
        runner.assertTransferCount(DebugFlow.REL_FAILURE, 0);

        runner.assertQueueNotEmpty();
        assertEquals(6, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void testFlowFileDefaultException() {
        runner.setProperty(DebugFlow.FF_EXCEPTION_ITERATIONS, "1");
        runner.assertValid();

        runner.enqueue(contents.get(0).getBytes(), attribs.get(0));

        exception.expectMessage(CoreMatchers.containsString("forced by org.apache.nifi.processors.standard.DebugFlow"));
        exception.expectCause(CoreMatchers.isA(RuntimeException.class));
        runner.run(2);
    }

    @Test
    public void testFlowFileNonDefaultException() {
        runner.setProperty(DebugFlow.FF_EXCEPTION_ITERATIONS, "1");
        runner.setProperty(DebugFlow.FF_EXCEPTION_CLASS, "java.lang.RuntimeException");
        runner.assertValid();

        runner.enqueue(contents.get(0).getBytes(), attribs.get(0));

        exception.expectMessage(CoreMatchers.containsString("forced by org.apache.nifi.processors.standard.DebugFlow"));
        exception.expectCause(CoreMatchers.isA(RuntimeException.class));
        runner.run(2);
    }

    @Test
    public void testFlowFileNPEException() {
        runner.setProperty(DebugFlow.FF_EXCEPTION_ITERATIONS, "1");
        runner.setProperty(DebugFlow.FF_EXCEPTION_CLASS, "java.lang.NullPointerException");
        runner.assertValid();

        runner.enqueue(contents.get(0).getBytes(), attribs.get(0));

        exception.expectMessage(CoreMatchers.containsString("forced by org.apache.nifi.processors.standard.DebugFlow"));
        exception.expectCause(CoreMatchers.isA(NullPointerException.class));
        runner.run(2);
    }

    @Test
    public void testFlowFileBadException() {
        runner.setProperty(DebugFlow.FF_EXCEPTION_ITERATIONS, "1");
        runner.setProperty(DebugFlow.FF_EXCEPTION_CLASS, "java.lang.NonExistantException");
        runner.assertNotValid();
    }

    @Test
    public void testFlowFileExceptionRollover() {
        runner.setProperty(DebugFlow.FF_EXCEPTION_ITERATIONS, "2");
        runner.assertValid();

        for (int n = 0; n < 6; n++) {
            runner.enqueue(contents.get(n).getBytes(), attribs.get(n));
        }

        exception.expectMessage(CoreMatchers.containsString("forced by org.apache.nifi.processors.standard.DebugFlow"));
        exception.expectCause(CoreMatchers.isA(RuntimeException.class));
        runner.run(8);
    }

    @Test
    public void testFlowFileAll() {
        runner.setProperty(DebugFlow.FF_SUCCESS_ITERATIONS, "1");
        runner.setProperty(DebugFlow.FF_FAILURE_ITERATIONS, "1");
        runner.setProperty(DebugFlow.FF_ROLLBACK_ITERATIONS, "1");
        runner.setProperty(DebugFlow.FF_ROLLBACK_YIELD_ITERATIONS, "1");
        runner.setProperty(DebugFlow.FF_ROLLBACK_PENALTY_ITERATIONS, "1");
        runner.setProperty(DebugFlow.FF_EXCEPTION_ITERATIONS, "1");
        runner.assertValid();

        for (int n = 0; n < 6; n++) {
            runner.enqueue(contents.get(n).getBytes(), attribs.get(n));
        }

        runner.run(5);
        runner.assertTransferCount(DebugFlow.REL_SUCCESS, 1);
        runner.assertTransferCount(DebugFlow.REL_FAILURE, 1);

        assertEquals(4, runner.getQueueSize().getObjectCount());
        assertTrue(isInContents(runner.getFlowFilesForRelationship(DebugFlow.REL_SUCCESS).get(0).toByteArray()));
        assertTrue(isInContents(runner.getFlowFilesForRelationship(DebugFlow.REL_FAILURE).get(0).toByteArray()));

        runner.run(2);
    }

    @Test
    public void testNoFlowFileZeroIterations() {
        runner.run(4);
    }

    @Test
    public void testNoFlowFileSkip() {
        runner.setProperty(DebugFlow.NO_FF_SKIP_ITERATIONS, "1");
        runner.assertValid();

        runner.run(4);
    }

    @Test
    public void testNoFlowFileDefaultException() {
        runner.setProperty(DebugFlow.NO_FF_EXCEPTION_ITERATIONS, "1");
        runner.assertValid();

        exception.expectMessage(CoreMatchers.containsString("forced by org.apache.nifi.processors.standard.DebugFlow"));
        exception.expectCause(CoreMatchers.isA(RuntimeException.class));
        runner.run(3);
    }

    @Test
    public void testNoFlowFileNonDefaultException() {
        runner.setProperty(DebugFlow.NO_FF_EXCEPTION_ITERATIONS, "1");
        runner.setProperty(DebugFlow.NO_FF_EXCEPTION_CLASS, "java.lang.RuntimeException");
        runner.assertValid();

        exception.expectMessage(CoreMatchers.containsString("forced by org.apache.nifi.processors.standard.DebugFlow"));
        exception.expectCause(CoreMatchers.isA(RuntimeException.class));
        runner.run(3);
    }

    @Test
    public void testNoFlowFileOtherException() {
        runner.setProperty(DebugFlow.NO_FF_EXCEPTION_ITERATIONS, "1");
        runner.setProperty(DebugFlow.NO_FF_EXCEPTION_CLASS, "java.lang.NullPointerException");
        runner.assertValid();

        exception.expectMessage(CoreMatchers.containsString("forced by org.apache.nifi.processors.standard.DebugFlow"));
        exception.expectCause(CoreMatchers.isA(NullPointerException.class));
        runner.run(3);
    }

    @Test
    public void testNoFlowFileBadException() {
        runner.setProperty(DebugFlow.NO_FF_EXCEPTION_ITERATIONS, "1");
        runner.setProperty(DebugFlow.NO_FF_EXCEPTION_CLASS, "java.lang.NonExistantException");
        runner.assertNotValid();
    }

    @Test
    public void testNoFlowFileYield() {
        runner.setProperty(DebugFlow.NO_FF_YIELD_ITERATIONS, "1");
        runner.assertValid();

        runner.run(4);
    }
}
