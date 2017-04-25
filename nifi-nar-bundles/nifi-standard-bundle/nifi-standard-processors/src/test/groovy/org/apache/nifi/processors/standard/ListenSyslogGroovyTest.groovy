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

import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSessionFactory
import org.apache.nifi.processors.standard.syslog.SyslogParser
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
class ListenSyslogGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ListenSyslogGroovyTest.class)

    static final String ZERO_LENGTH_MESSAGE = "     \n"

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
        final ListenSyslog proc = new ListenSyslog()
        final TestRunner runner = TestRunners.newTestRunner(proc)
        runner.setProperty(ListenSyslog.PROTOCOL, ListenSyslog.TCP_VALUE.getValue())
        runner.setProperty(ListenSyslog.PORT, "0")

        // schedule to start listening on a random port
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory()
        final ProcessContext context = runner.getProcessContext()
        proc.onScheduled(context)

        // Inject a SyslogParser which will always return null
        def nullEventParser = [parseEvent: { byte[] bytes, String sender ->
            logger.mock("Regardless of input bytes: [${Hex.toHexString(bytes)}] and sender: [${sender}], this parser will return null")
            return null
        }] as SyslogParser
        proc.parser = nullEventParser

        final int numMessages = 10
        final int port = proc.getPort()
        Assert.assertTrue(port > 0)

        // write some TCP messages to the port in the background
        final Thread sender = new Thread(new TestListenSyslog.SingleConnectionSocketSender(port, numMessages, 100, ZERO_LENGTH_MESSAGE))
        sender.setDaemon(true)
        sender.start()

        // Act

        // call onTrigger until we read all messages, or 30 seconds passed
        try {
            int numFailed = 0
            long timeout = System.currentTimeMillis() + 30000

            while (numFailed < numMessages && System.currentTimeMillis() < timeout) {
                Thread.sleep(50)
                proc.onTrigger(context, processSessionFactory)
                numFailed = runner.getFlowFilesForRelationship(ListenSyslog.REL_INVALID).size()
            }

            int numSuccess = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).size()
            logger.info("Transferred " + numSuccess + " to SUCCESS and " + numFailed + " to INVALID")

            // Assert

            // all messages should be transferred to invalid
            Assert.assertEquals("Did not process all the messages", numMessages, numFailed)

        } finally {
            // unschedule to close connections
            proc.onUnscheduled()
        }
    }
}
