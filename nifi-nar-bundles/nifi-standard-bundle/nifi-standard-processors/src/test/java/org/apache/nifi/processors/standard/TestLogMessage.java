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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class TestLogMessage {

    private TestableLogMessage testableLogMessage;
    private TestRunner runner;

    private static class TestableLogMessage extends LogMessage {

        MockComponentLog getMockComponentLog() {
            ComponentLog mockLog = getLogger();

            if (!(mockLog instanceof MockComponentLog)) {
                throw new IllegalStateException("Logger is expected to be MockComponentLog, but was: " +
                        mockLog.getClass());
            }

            return (MockComponentLog) mockLog;
        }


    }

    @Before
    public void before() throws InitializationException {
        testableLogMessage = new TestableLogMessage();
        runner = TestRunners.newTestRunner(testableLogMessage);

    }

    @After
    public void after() throws InitializationException {
        runner.shutdown();
    }

    @Test
    public void testInfoMessageLogged() throws InitializationException, IOException {

        runner.setProperty(LogMessage.LOG_MESSAGE, "This should help the operator to follow the flow: ${foobar}");
        runner.setProperty(LogMessage.LOG_LEVEL, LogMessage.MessageLogLevel.info.toString());

        HashMap<String, String> flowAttributes = new HashMap<>();
        flowAttributes.put("foobar", "baz");

        runner.enqueue("This is a message!", flowAttributes);
        runner.setValidateExpressionUsage(false);

        runner.run();

        List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(LogMessage.REL_SUCCESS);
        Assert.assertEquals(1, successFlowFiles.size());

        MockComponentLog mockComponentLog = testableLogMessage.getMockComponentLog();
        List<org.apache.nifi.util.LogMessage> infoMessages = mockComponentLog.getInfoMessages();
        Assert.assertEquals(1, infoMessages.size());
        Assert.assertTrue(infoMessages.get(0).getMsg()
                .endsWith("This should help the operator to follow the flow: baz"));


        Assert.assertTrue(mockComponentLog.getTraceMessages().isEmpty());
        Assert.assertTrue(mockComponentLog.getDebugMessages().isEmpty());
        Assert.assertTrue(mockComponentLog.getWarnMessages().isEmpty());
        Assert.assertTrue(mockComponentLog.getErrorMessages().isEmpty());
    }

    @Test
    public void testInfoMessageWithPrefixLogged() throws InitializationException, IOException {

        runner.setProperty(LogMessage.LOG_PREFIX, "FOOBAR>>>");
        runner.setProperty(LogMessage.LOG_MESSAGE, "This should help the operator to follow the flow: ${foobar}");
        runner.setProperty(LogMessage.LOG_LEVEL, LogMessage.MessageLogLevel.info.toString());

        HashMap<String, String> flowAttributes = new HashMap<>();
        flowAttributes.put("foobar", "baz");

        runner.enqueue("This is a message!", flowAttributes);
        runner.setValidateExpressionUsage(false);

        runner.run();

        List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(LogMessage.REL_SUCCESS);
        Assert.assertEquals(1, successFlowFiles.size());

        MockComponentLog mockComponentLog = testableLogMessage.getMockComponentLog();
        List<org.apache.nifi.util.LogMessage> infoMessages = mockComponentLog.getInfoMessages();
        Assert.assertEquals(1, infoMessages.size());
        Assert.assertTrue(infoMessages.get(0).getMsg()
                .endsWith("FOOBAR>>>This should help the operator to follow the flow: baz"));



        Assert.assertTrue(mockComponentLog.getTraceMessages().isEmpty());
        Assert.assertTrue(mockComponentLog.getDebugMessages().isEmpty());
        Assert.assertTrue(mockComponentLog.getWarnMessages().isEmpty());
        Assert.assertTrue(mockComponentLog.getErrorMessages().isEmpty());
    }

}
