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
package org.apache.nifi.processors.standard

import org.apache.nifi.syslog.parsers.SyslogParser
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.bouncycastle.util.encoders.Hex
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class ParseSyslogGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ParseSyslogGroovyTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    @Test
    void testShouldHandleZeroLengthUDP() throws Exception {
        // Arrange
        final ParseSyslog proc = new ParseSyslog()
        final TestRunner runner = TestRunners.newTestRunner(proc)
        runner.setProperty(ParseSyslog.CHARSET, ParseSyslog.CHARSET.defaultValue)

        // Inject a SyslogParser which will always return null
        def nullEventParser = [parseEvent: { byte[] bytes, String sender ->
            logger.mock("Regardless of input bytes: [${Hex.toHexString(bytes)}] and sender: [${sender}], this parser will return null")
            return null
        }] as SyslogParser
        proc.parser = nullEventParser

        final int numMessages = 10

        // Act
        numMessages.times {
            runner.enqueue("Doesn't matter what is enqueued here")
        }
        runner.run(numMessages)

        int numFailed = runner.getFlowFilesForRelationship(ParseSyslog.REL_FAILURE).size()
        int numSuccess = runner.getFlowFilesForRelationship(ParseSyslog.REL_SUCCESS).size()
        logger.info("Transferred " + numSuccess + " to SUCCESS and " + numFailed + " to FAILURE")

        // Assert

        // all messages should be transferred to invalid
        Assert.assertEquals("Did not process all the messages", numMessages, numFailed)
    }
}
