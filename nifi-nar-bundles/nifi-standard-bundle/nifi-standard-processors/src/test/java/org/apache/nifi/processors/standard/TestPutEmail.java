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

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.nifi.processors.email.ExtractEmailAttachments;
import org.apache.nifi.processors.email.ExtractEmailHeaders;
import org.apache.nifi.processors.email.ListenSMTP;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



public class TestPutEmail {

    TestRunner runner;
    TestRunner smtpRunner;
    TestRunner extractHeadersRunner;
    TestRunner extractAttachmentsRunner;
    int port;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(PutEmail.class);
        smtpRunner = TestRunners.newTestRunner(ListenSMTP.class);
        extractHeadersRunner = TestRunners.newTestRunner(ExtractEmailHeaders.class);
        extractAttachmentsRunner = TestRunners.newTestRunner(ExtractEmailAttachments.class);

        port = NetworkUtils.availablePort();
        smtpRunner.setProperty("SMTP_PORT", String.valueOf(port));
        smtpRunner.setProperty("SMTP_MAXIMUM_CONNECTIONS", "3");
        smtpRunner.setProperty("SMTP_TIMEOUT", "10 seconds");
        smtpRunner.run(1,false);

    }



    @Test
    public void testExceptionWhenSending() {
        // verifies that files are routed to failure when Transport.send() throws a MessagingException
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "host-doesnt-exist123-att-all");
        runner.setProperty(PutEmail.FROM, "test@nifi.apache.org");
        runner.setProperty(PutEmail.TO, "test@nifi.apache.org");
        runner.setProperty(PutEmail.MESSAGE, "Message Body");

        //

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue("Some Text".getBytes(), attributes);

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_FAILURE);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(PutEmail.REL_FAILURE);
        assertEquals("Expected an attempt to send a single message", 1, results.size());
    }

    @Test
    public void testOutgoingMessage() throws Exception {
        // verifies that are set on the outgoing Message correctly
        runner.setProperty(PutEmail.SMTP_AUTH, "false");
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "localhost");
        runner.setProperty(PutEmail.SMTP_PORT, String.valueOf(port));
        runner.setProperty(PutEmail.HEADER_XMAILER, "TestingNiFi");
        runner.setProperty(PutEmail.FROM, "test@nifi.apache.org");
        runner.setProperty(PutEmail.MESSAGE, "Message Body");
        runner.setProperty(PutEmail.TO, "recipient@nifi.apache.org");

        runner.enqueue("Some Text".getBytes());

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_SUCCESS);


        // Get the results from the ListenSMTP
        List<MockFlowFile> results = smtpRunner.getFlowFilesForRelationship(PutEmail.REL_SUCCESS);

        // Verify that the Message was populated correctly
        assertEquals("Expected a single message to be sent", 1, results.size());
        MockFlowFile result = results.get(0);

        assertEquals("test@nifi.apache.org", result.getAttribute("smtp.from"));
        assertEquals("recipient@nifi.apache.org", result.getAttribute("smtp.recipient.0"));
    }

    @Test
    public void testOutgoingMessageWithOptionalProperties() throws Exception {
        // verifies that optional attributes are set on the outgoing Message correctly
        runner.setProperty(PutEmail.SMTP_AUTH, "false");
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "localhost");
        runner.setProperty(PutEmail.SMTP_PORT, String.valueOf(port));
        runner.setProperty(PutEmail.HEADER_XMAILER, "TestingNiFi");
        runner.setProperty(PutEmail.FROM, "${from}");
        runner.setProperty(PutEmail.MESSAGE, "${message}");
        runner.setProperty(PutEmail.TO, "${to}");
        runner.setProperty(PutEmail.BCC, "${bcc}");
        runner.setProperty(PutEmail.CC, "${cc}");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("from", "test@nifi.apache.org <NiFi>");
        attributes.put("message", "the message body");
        attributes.put("to", "to@nifi.apache.org");
        attributes.put("bcc", "bcc@nifi.apache.org");
        attributes.put("cc", "cc@nifi.apache.org");
        runner.enqueue("Some Text".getBytes(), attributes);

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_SUCCESS);

        // Get the results from the ListenSMTP
        List<MockFlowFile> results = smtpRunner.getFlowFilesForRelationship(PutEmail.REL_SUCCESS);

        // Verify that the Message was populated correctly
        assertEquals("Expected a single message to be sent", 1, results.size());
        MockFlowFile result = results.get(0);

        // Insert into ExtractEmailHeaders to process desired fields
        extractHeadersRunner.enqueue(result);
        extractHeadersRunner.run();
        // get results of extraction
        results = extractHeadersRunner.getFlowFilesForRelationship(ExtractEmailHeaders.REL_SUCCESS);
        assertEquals("Expected the message to be properly parsed", 1, results.size());
        result = results.get(0);

        assertEquals("\"test@nifi.apache.org\" <NiFi>", result.getAttribute("email.headers.from.0"));
        assertEquals("X-Mailer Header", "TestingNiFi", result.getAttribute("email.headers.x-mailer"));
        result.equals("the message body");
        assertEquals("to@nifi.apache.org", result.getAttribute("email.headers.to.0"));
    }

    @Test
    public void testInvalidAddress() throws Exception {
        // verifies that unparsable addresses lead to the flow file being routed to failure
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "smtp-host");
        runner.setProperty(PutEmail.HEADER_XMAILER, "TestingNiFi");
        runner.setProperty(PutEmail.FROM, "test@nifi.apache.org <invalid");
        runner.setProperty(PutEmail.MESSAGE, "Message Body");
        runner.setProperty(PutEmail.TO, "recipient@nifi.apache.org");

        runner.enqueue("Some Text".getBytes());

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_FAILURE);

        assertEquals("Expected no messages to be sent", 1, runner.getFlowFilesForRelationship(PutEmail.REL_FAILURE).size());
    }

    @Test
    public void testEmptyFrom() throws Exception {
        // verifies that if the FROM property evaluates to an empty string at
        // runtime the flow file is transferred to failure.
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "smtp-host");
        runner.setProperty(PutEmail.HEADER_XMAILER, "TestingNiFi");
        runner.setProperty(PutEmail.FROM, "${MISSING_PROPERTY}");
        runner.setProperty(PutEmail.MESSAGE, "Message Body");
        runner.setProperty(PutEmail.TO, "recipient@nifi.apache.org");

        runner.enqueue("Some Text".getBytes());

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_FAILURE);

        assertEquals("Expected no messages to be sent", 1, runner.getFlowFilesForRelationship(PutEmail.REL_FAILURE).size());
        final LogMessage logMessage = runner.getLogger().getErrorMessages().get(0);
        assertTrue(((String)logMessage.getArgs()[2]).contains("Required property 'From' evaluates to an empty string"));
    }

    @Test
    public void testOutgoingMessageAttachment() throws Exception {
        // verifies that are set on the outgoing Message correctly
        runner.setProperty(PutEmail.SMTP_AUTH, "false");
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "localhost");
        runner.setProperty(PutEmail.SMTP_PORT, String.valueOf(port));
        runner.setProperty(PutEmail.HEADER_XMAILER, "TestingNiFi");
        runner.setProperty(PutEmail.FROM, "test@nifi.apache.org");
        runner.setProperty(PutEmail.MESSAGE, "This is the message body");
        runner.setProperty(PutEmail.ATTACH_FILE, "true");
        runner.setProperty(PutEmail.CONTENT_TYPE, "text/html");
        runner.setProperty(PutEmail.TO, "recipient@nifi.apache.org");

        runner.enqueue("Some text".getBytes());

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_SUCCESS);

        // Verify that the Message was populated correctly
        assertEquals("Expected a single message to be sent", 1, smtpRunner.getFlowFilesForRelationship(PutEmail.REL_SUCCESS).size());

        MockFlowFile message;

        message = smtpRunner.getFlowFilesForRelationship(PutEmail.REL_SUCCESS).get(0);

        extractHeadersRunner.enqueue(message);
        extractHeadersRunner.run(1, false);
        extractHeadersRunner.shutdown();

        message = extractHeadersRunner.getFlowFilesForRelationship(ExtractEmailHeaders.REL_SUCCESS).get(0);

        assertEquals("test@nifi.apache.org", message.getAttribute("email.headers.from.0"));
        assertEquals("X-Mailer Header", "TestingNiFi", message.getAttribute("email.headers.x-mailer"));

        message = extractHeadersRunner.getFlowFilesForRelationship(ExtractEmailHeaders.REL_SUCCESS).get(0);

        // Because TestRunner isn't happy to process another TestRunner, convert the message content into byte array and
        // submit to the next stage
        extractAttachmentsRunner.enqueue(message.toByteArray());
        extractAttachmentsRunner.run();

        final List<MockFlowFile> attachments = extractAttachmentsRunner.getFlowFilesForRelationship(ExtractEmailAttachments.REL_ATTACHMENTS);

        // Assert the attachment was properly coded and extracted
        attachments.get(0).assertContentEquals("Some text");


        String encodedBody;

        // Confirm positive match
        encodedBody = StringUtils.newStringUtf8(Base64.encodeBase64("This is the message body".getBytes()));
        Assert.assertThat(new String(message.toByteArray()), CoreMatchers.containsString(encodedBody));

        // Double check
        encodedBody = StringUtils.newStringUtf8(Base64.encodeBase64("This is NOT the message body".getBytes()));
        Assert.assertThat(new String(message.toByteArray()), CoreMatchers.not(CoreMatchers.containsString(encodedBody)));

    }

}
