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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestExecuteProcess {

    @Test
    public void testSplitArgs() {
        final List<String> nullArgs = ExecuteProcess.splitArgs(null);
        assertNotNull(nullArgs);
        assertTrue(nullArgs.isEmpty());

        final List<String> zeroArgs = ExecuteProcess.splitArgs("  ");
        assertNotNull(zeroArgs);
        assertTrue(zeroArgs.isEmpty());

        final List<String> singleArg = ExecuteProcess.splitArgs("    hello   ");
        assertEquals(1, singleArg.size());
        assertEquals("hello", singleArg.get(0));

        final List<String> twoArg = ExecuteProcess.
                splitArgs("   hello    good-bye   ");
        assertEquals(2, twoArg.size());
        assertEquals("hello", twoArg.get(0));
        assertEquals("good-bye", twoArg.get(1));

        final List<String> singleQuotedArg = ExecuteProcess.
                splitArgs("  \"hello\" ");
        assertEquals(1, singleQuotedArg.size());
        assertEquals("hello", singleQuotedArg.get(0));

        final List<String> twoQuotedArg = ExecuteProcess.
                splitArgs("   hello \"good   bye\"");
        assertEquals(2, twoQuotedArg.size());
        assertEquals("hello", twoQuotedArg.get(0));
        assertEquals("good   bye", twoQuotedArg.get(1));
    }

    @Test
    public void testEcho() {
        System.
                setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "TRACE");

        final TestRunner runner = TestRunners.
                newTestRunner(ExecuteProcess.class);
        runner.setProperty(ExecuteProcess.COMMAND, "echo");
        runner.setProperty(ExecuteProcess.COMMAND_ARGUMENTS, "test-args");
        runner.setProperty(ExecuteProcess.BATCH_DURATION, "500 millis");

        runner.run();

        final List<MockFlowFile> flowFiles = runner.
                getFlowFilesForRelationship(ExecuteProcess.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile);
            System.out.println(new String(flowFile.toByteArray()));
        }
    }
}
