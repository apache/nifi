package org.apache.nifi.processors.standard

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
class ParseSyslogGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ParseSyslogGroovyTest.class)

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
