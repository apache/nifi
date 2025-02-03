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

import jakarta.mail.BodyPart;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage.RecipientType;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.internet.MimeUtility;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPutEmail {

    /**
     * Extension to PutEmail that stubs out the calls to
     * Transport.sendMessage().
     *
     * <p>
     * All sent messages are records in a list available via the
     * {@link #getMessages()} method.</p>
     * <p> Calling
     * {@link #setException(MessagingException)} will cause the supplied exception to be
     * thrown when sendMessage is invoked.
     * </p>
     */
    private static final class PutEmailExtension extends PutEmail {
        private MessagingException e;
        private final ArrayList<Message> messages = new ArrayList<>();

        @Override
        protected void send(Message msg) throws MessagingException {
            messages.add(msg);
            if (this.e != null) {
                throw e;
            }
        }

        void setException(final MessagingException e) {
            this.e = e;
        }

        List<Message> getMessages() {
            return messages;
        }
    }

    PutEmailExtension processor;
    TestRunner runner;

    @BeforeEach
    public void setup() {
        processor = new PutEmailExtension();
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testExceptionWhenSending() {
        // verifies that files are routed to failure when Transport.send() throws a MessagingException
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "host-doesnt-exist123");
        runner.setProperty(PutEmail.FROM, "test@apache.org");
        runner.setProperty(PutEmail.TO, "test@apache.org");
        runner.setProperty(PutEmail.MESSAGE, "Message Body");

        processor.setException(new MessagingException("Forced failure from send()"));

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue("Some Text".getBytes(), attributes);

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_FAILURE);
        assertEquals(1, processor.getMessages().size(), "Expected an attempt to send a single message");
    }

    @Test
    public void testOutgoingMessage() throws Exception {
        // verifies that are set on the outgoing Message correctly
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "smtp-host");
        runner.setProperty(PutEmail.HEADER_XMAILER, "TestingNiFi");
        runner.setProperty(PutEmail.FROM, "test@apache.org");
        runner.setProperty(PutEmail.MESSAGE, "Message Body");
        runner.setProperty(PutEmail.TO, "recipient@apache.org");
        runner.setProperty(PutEmail.INPUT_CHARACTER_SET, StandardCharsets.UTF_8.name());

        runner.enqueue("Some Text".getBytes());

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_SUCCESS);

        // Verify that the Message was populated correctly
        assertEquals(1, processor.getMessages().size(), "Expected a single message to be sent");
        Message message = processor.getMessages().get(0);
        assertEquals("test@apache.org", message.getFrom()[0].toString());
        assertEquals("TestingNiFi", message.getHeader("X-Mailer")[0], "X-Mailer Header");
        assertEquals("Message Body", getMessageText(message, StandardCharsets.UTF_8));
        assertEquals("recipient@apache.org", message.getRecipients(RecipientType.TO)[0].toString());
        assertNull(message.getRecipients(RecipientType.BCC));
        assertNull(message.getRecipients(RecipientType.CC));
    }

    @Test
    public void testOAuth() throws Exception {
        // GIVEN
        String oauthServiceID = "oauth-access-token-provider";
        String access_token = "access_token_123";

        OAuth2AccessTokenProvider oauthService = mock(OAuth2AccessTokenProvider.class, RETURNS_DEEP_STUBS);
        when(oauthService.getIdentifier()).thenReturn(oauthServiceID);
        when(oauthService.getAccessDetails().getAccessToken()).thenReturn(access_token);
        runner.addControllerService(oauthServiceID, oauthService);
        runner.enableControllerService(oauthService);

        runner.setProperty(PutEmail.SMTP_HOSTNAME, "unimportant");
        runner.setProperty(PutEmail.FROM, "unimportant");
        runner.setProperty(PutEmail.TO, "unimportant");
        runner.setProperty(PutEmail.MESSAGE, "unimportant");
        runner.setProperty(PutEmail.AUTHORIZATION_MODE, PutEmail.OAUTH_AUTHORIZATION_MODE);
        runner.setProperty(PutEmail.OAUTH2_ACCESS_TOKEN_PROVIDER, oauthServiceID);

        // WHEN
        runner.enqueue("Unimportant flowfile content".getBytes());

        runner.run();

        // THEN
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_SUCCESS);

        assertEquals(1, processor.getMessages().size(), "Expected a single message to be sent");
        Message message = processor.getMessages().get(0);
        assertEquals("XOAUTH2", message.getSession().getProperty("mail.smtp.auth.mechanisms"));
        assertEquals("access_token_123", message.getSession().getProperty("mail.smtp.password"));
    }

    @Test
    public void testOutgoingMessageWithOptionalProperties() throws Exception {
        // verifies that optional attributes are set on the outgoing Message correctly
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "smtp-host");
        runner.setProperty(PutEmail.HEADER_XMAILER, "TestingNíFiNonASCII");
        runner.setProperty(PutEmail.FROM, "${from}");
        runner.setProperty(PutEmail.MESSAGE, "${message}");
        runner.setProperty(PutEmail.TO, "${to}");
        runner.setProperty(PutEmail.BCC, "${bcc}");
        runner.setProperty(PutEmail.CC, "${cc}");
        runner.setProperty(PutEmail.REPLY_TO, "${reply-to}");
        runner.setProperty(PutEmail.ATTRIBUTE_NAME_REGEX, "Precedence.*");
        runner.setProperty(PutEmail.INPUT_CHARACTER_SET, StandardCharsets.UTF_8.name());

        Map<String, String> attributes = new HashMap<>();
        attributes.put("from", "test@apache.org <NiFi>");
        attributes.put("message", "the message body");
        attributes.put("to", "to@apache.org");
        attributes.put("bcc", "bcc@apache.org");
        attributes.put("cc", "cc@apache.org");
        attributes.put("reply-to", "replytome@apache.org");
        attributes.put("Precedence", "bulk");
        attributes.put("PrecedenceEncodeDecodeTest", "búlk");
        runner.enqueue("Some Text".getBytes(), attributes);

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_SUCCESS);

        // Verify that the Message was populated correctly
        assertEquals(1, processor.getMessages().size(), "Expected a single message to be sent");
        Message message = processor.getMessages().get(0);
        assertEquals("\"test@apache.org\" <NiFi>", message.getFrom()[0].toString());
        assertEquals("TestingNíFiNonASCII", MimeUtility.decodeText(message.getHeader("X-Mailer")[0]), "X-Mailer Header");
        assertEquals("the message body", getMessageText(message, StandardCharsets.UTF_8));
        assertEquals(1, message.getRecipients(RecipientType.TO).length);
        assertEquals("to@apache.org", message.getRecipients(RecipientType.TO)[0].toString());
        assertEquals(1, message.getRecipients(RecipientType.BCC).length);
        assertEquals("bcc@apache.org", message.getRecipients(RecipientType.BCC)[0].toString());
        assertEquals(1, message.getRecipients(RecipientType.CC).length);
        assertEquals("cc@apache.org", message.getRecipients(RecipientType.CC)[0].toString());
        assertEquals(1, message.getReplyTo().length);
        assertEquals("replytome@apache.org", message.getReplyTo()[0].toString());
        assertEquals("bulk", MimeUtility.decodeText(message.getHeader("Precedence")[0]));
        assertEquals("búlk", MimeUtility.decodeText(message.getHeader("PrecedenceEncodeDecodeTest")[0]));
    }

    @Test
    public void testInvalidAddress() {
        // verifies that unparsable addresses lead to the flow file being routed to failure
        setRequiredProperties(runner);
        runner.setProperty(PutEmail.FROM, "test@apache.org <invalid");
        runner.setProperty(PutEmail.MESSAGE, "Message Body");

        runner.enqueue("Some Text".getBytes());

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_FAILURE);

        assertEquals(0, processor.getMessages().size(), "Expected no messages to be sent");
    }

    @Test
    public void testEmptyFrom() {
        // verifies that if the FROM property evaluates to an empty string at
        // runtime the flow file is transferred to failure.
        setRequiredProperties(runner);
        runner.setProperty(PutEmail.FROM, "${MISSING_PROPERTY}");
        runner.setProperty(PutEmail.MESSAGE, "Message Body");

        runner.enqueue("Some Text".getBytes());

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_FAILURE);

        assertEquals(0, processor.getMessages().size(), "Expected no messages to be sent");
        assertFalse(runner.getLogger().getErrorMessages().isEmpty());
    }

    @Test
    public void testOutgoingMessageAttachment() throws Exception {
        // verifies that are set on the outgoing Message correctly
        setRequiredProperties(runner);
        runner.setProperty(PutEmail.MESSAGE, "Message Body");
        runner.setProperty(PutEmail.ATTACH_FILE, "true");
        runner.setProperty(PutEmail.CONTENT_TYPE, "text/html");
        runner.setProperty(PutEmail.TO, "recipient@apache.org");
        runner.setProperty(PutEmail.INPUT_CHARACTER_SET, StandardCharsets.UTF_8.name());

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "test한的ほу́.pdf");
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/pdf");
        runner.enqueue("Some text".getBytes(), attributes);

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_SUCCESS);

        // Verify that the Message was populated correctly
        assertEquals(1, processor.getMessages().size(), "Expected a single message to be sent");
        Message message = processor.getMessages().get(0);
        assertEquals("test@apache.org", message.getFrom()[0].toString());
        assertEquals("TestingNiFi", message.getHeader("X-Mailer")[0], "X-Mailer Header");
        assertEquals("recipient@apache.org", message.getRecipients(RecipientType.TO)[0].toString());

        assertInstanceOf(MimeMultipart.class, message.getContent());

        final MimeMultipart multipart = (MimeMultipart) message.getContent();

        assertEquals("Message Body", getMessageText(message, StandardCharsets.UTF_8));

        final BodyPart attachPart = multipart.getBodyPart(1);
        final InputStream attachIs = attachPart.getDataHandler().getInputStream();
        final String text = IOUtils.toString(attachIs, StandardCharsets.UTF_8);
        final String mimeType = attachPart.getDataHandler().getContentType();
        assertEquals("test한的ほу́.pdf", MimeUtility.decodeText(attachPart.getFileName()));
        assertEquals("application/pdf", mimeType);
        assertEquals("Some text", text);

        assertNull(message.getRecipients(RecipientType.BCC));
        assertNull(message.getRecipients(RecipientType.CC));
    }

    @Test
    public void testOutgoingMessageWithFlowfileContent() throws Exception {
        // verifies that are set on the outgoing Message correctly
        setRequiredProperties(runner);
        runner.setProperty(PutEmail.MESSAGE, "${body}");
        runner.setProperty(PutEmail.CC, "recipientcc@apache.org,anothercc@apache.org");
        runner.setProperty(PutEmail.BCC, "recipientbcc@apache.org,anotherbcc@apache.org");
        runner.setProperty(PutEmail.CONTENT_AS_MESSAGE, "${sendContent}");
        runner.setProperty(PutEmail.INPUT_CHARACTER_SET, StandardCharsets.UTF_8.name());

        Map<String, String> attributes = new HashMap<>();
        attributes.put("sendContent", "true");
        attributes.put("body", "Message Body");

        runner.enqueue("Some Text".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_SUCCESS);

        // Verify that the Message was populated correctly
        assertEquals(1, processor.getMessages().size(), "Expected a single message to be sent");
        Message message = processor.getMessages().get(0);
        assertEquals("test@apache.org", message.getFrom()[0].toString());
        assertEquals("from@apache.org", message.getFrom()[1].toString());
        assertEquals("TestingNiFi", message.getHeader("X-Mailer")[0], "X-Mailer Header");
        assertEquals("Some Text", getMessageText(message, StandardCharsets.UTF_8));
        assertEquals("recipient@apache.org", message.getRecipients(RecipientType.TO)[0].toString());
        assertEquals("another@apache.org", message.getRecipients(RecipientType.TO)[1].toString());
        assertEquals("recipientcc@apache.org", message.getRecipients(RecipientType.CC)[0].toString());
        assertEquals("anothercc@apache.org", message.getRecipients(RecipientType.CC)[1].toString());
        assertEquals("recipientbcc@apache.org", message.getRecipients(RecipientType.BCC)[0].toString());
        assertEquals("anotherbcc@apache.org", message.getRecipients(RecipientType.BCC)[1].toString());
    }

    @Test
    public void testValidDynamicMailProperties() {
        setRequiredProperties(runner);
        runner.setProperty(PutEmail.MESSAGE, "${body}");
        runner.setProperty(PutEmail.CONTENT_AS_MESSAGE, "${sendContent}");

        runner.setProperty("mail.smtp.timeout", "sample dynamic smtp property");
        runner.setProperty("mail.smtps.timeout", "sample dynamic smtps property");
        runner.assertValid();
    }

    @Test
    public void testInvalidDynamicMailPropertyName() {
        setRequiredProperties(runner);
        runner.setProperty(PutEmail.MESSAGE, "${body}");
        runner.setProperty(PutEmail.CONTENT_AS_MESSAGE, "${sendContent}");

        runner.setProperty("mail.", "sample_value");
    }

    @Test
    public void testOverwritingDynamicMailProperty() {
        setRequiredProperties(runner);
        runner.setProperty(PutEmail.MESSAGE, "${body}");
        runner.setProperty(PutEmail.CONTENT_AS_MESSAGE, "${sendContent}");

        runner.setProperty("mail.smtp.user", "test-user-value");
        runner.assertNotValid();
    }

    @Test
    public void testUnrecognizedCharset() {
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "smtp-host");
        runner.setProperty(PutEmail.HEADER_XMAILER, "TestingNiFi");
        runner.setProperty(PutEmail.FROM, "test@apache.org");
        runner.setProperty(PutEmail.MESSAGE, "test message");
        runner.setProperty(PutEmail.TO, "recipient@apache.org");

        // not one of the recognized charsets
        runner.setProperty(PutEmail.INPUT_CHARACTER_SET, "NOT A CHARACTER SET");

        runner.assertNotValid();
    }

    @Test
    public void testPutEmailWithMismatchedCharset() throws Exception {
        // String specifically chosen to have characters encoded differently in US_ASCII and UTF_8
        final String rawString = "SoftwÄrë Ënginëër Ön NiFi";
        final byte[] rawBytes = rawString.getBytes(StandardCharsets.US_ASCII);
        final byte[] rawBytesUTF8 = rawString.getBytes(StandardCharsets.UTF_8);

        // verify that the message bytes are different (some messages are not)
        assertNotEquals(rawBytes, rawBytesUTF8);

        runner.setProperty(PutEmail.SMTP_HOSTNAME, "smtp-host");
        runner.setProperty(PutEmail.HEADER_XMAILER, "TestingNiFi");
        runner.setProperty(PutEmail.FROM, "test@apache.org");
        runner.setProperty(PutEmail.MESSAGE, new String(rawBytesUTF8, StandardCharsets.US_ASCII));
        runner.setProperty(PutEmail.TO, "recipient@apache.org");
        runner.setProperty(PutEmail.INPUT_CHARACTER_SET, StandardCharsets.UTF_8.name());

        runner.enqueue("Some Text".getBytes());

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_SUCCESS);

        // Verify that the Message was populated correctly
        assertEquals(1, processor.getMessages().size(), "Expected a single message to be sent");
        Message message = processor.getMessages().get(0);
        final String retrievedMessageText = getMessageText(message, StandardCharsets.UTF_8);
        assertNotEquals(rawString, retrievedMessageText);
    }

    private void setRequiredProperties(final TestRunner runner) {
        // values here may be overridden in some tests
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "smtp-host");
        runner.setProperty(PutEmail.HEADER_XMAILER, "TestingNiFi");
        runner.setProperty(PutEmail.FROM, "test@apache.org,from@apache.org");
        runner.setProperty(PutEmail.TO, "recipient@apache.org,another@apache.org");
    }

    private String getMessageText(final Message message, final Charset charset) throws Exception {
        if (message.getContent() instanceof MimeMultipart) {
            final MimeMultipart multipart = (MimeMultipart) message.getContent();
            final BodyPart part = multipart.getBodyPart(0);
            final InputStream is = part.getDataHandler().getInputStream();
            final String encoding = StandardCharsets.US_ASCII.equals(charset) ? "7bit" : "base64";
            final byte[] decodedTextBytes = "base64".equals(encoding) ? Base64.decodeBase64(IOUtils.toByteArray(is)) : IOUtils.toByteArray(is);
            final String decodedText = StringUtils.newString(decodedTextBytes, charset.name());
            return decodedText;
        } else {
            return (String) message.getContent();
        }
    }
}
