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

package org.apache.nifi.processors.email;

import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestExtractEmailAttachments {
    private static final String EXPECTED_CONTENT_TYPE_KEY = ExtractEmailAttachments.ATTACHMENT_HEADER_ATTRIBUTE_PREFIX + "content-type";
    private static final String EXPECTED_CONTENT_DISPOSITION_KEY = ExtractEmailAttachments.ATTACHMENT_HEADER_ATTRIBUTE_PREFIX + "content-disposition";

    final String from = "Alice <alice@nifi.apache.org>";
    final String to = "bob@nifi.apache.org";
    final String subject = "Just a test email";
    final String message = "Test test test chocolate";
    final String hostName = "bermudatriangle";

    final GenerateAttachment attachmentGenerator = new GenerateAttachment(from, to, subject, message, hostName);

    TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(ExtractEmailAttachments.class);
    }

    @Test
    public void testValidEmailWithAttachments() {
        byte[] withAttachment = attachmentGenerator.withAttachments(1);

        runner.enqueue(withAttachment);
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 1);
        // Have a look at the attachments...
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailAttachments.REL_ATTACHMENTS);
        final MockFlowFile split = splits.getFirst();
        split.assertAttributeEquals("filename", "pom.xml-0");
        final Map<String, String> expected = Map.of(
                EXPECTED_CONTENT_DISPOSITION_KEY, "attachment; filename=\"pom.xml-0\"",
                EXPECTED_CONTENT_TYPE_KEY, "text/plain; charset=utf-8"
        );
        assertAttachmentHeaderAttributes(split, expected);
    }

    @Test
    public void testValidEmailWithMultipleAttachments() {
        int amount = 3;
        byte[] withAttachment = attachmentGenerator.withAttachments(amount);

        runner.enqueue(withAttachment);
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, amount);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailAttachments.REL_ATTACHMENTS);
        final String expectedContentType = "text/plain; charset=utf-8";
        final List<Map<String, String>> expectedHeaderAttachmentAttributes = List.of(
                Map.of(EXPECTED_CONTENT_DISPOSITION_KEY, "attachment; filename=\"pom.xml-0\"", EXPECTED_CONTENT_TYPE_KEY, expectedContentType),
                Map.of(EXPECTED_CONTENT_DISPOSITION_KEY, "attachment; filename=\"pom.xml-1\"", EXPECTED_CONTENT_TYPE_KEY, expectedContentType),
                Map.of(EXPECTED_CONTENT_DISPOSITION_KEY, "attachment; filename=\"pom.xml-2\"", EXPECTED_CONTENT_TYPE_KEY, expectedContentType)
        );

        for (int index = 0; index < amount; index++) {
            final MockFlowFile split = splits.get(index);
            split.assertAttributeEquals("filename", "pom.xml-" + index);
            assertAttachmentHeaderAttributes(split, expectedHeaderAttachmentAttributes.get(index));
        }
    }

    @Test
    public void testValidEmailWithoutAttachments() {
        byte[] simpleEmail = attachmentGenerator.simpleMessage();

        runner.enqueue(simpleEmail);
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 0);
    }

    @Test
    public void testInvalidEmail() {
        runner.enqueue("test test test chocolate".getBytes());
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 0);
    }

    @Test
    public void testDeeplyNestedMultipartMimeMessage() throws Exception {
        final byte[] deeplyNestedMultipartMimeMessage = generateDeeplyNestedMultipartMimeMessage();
        runner.enqueue(deeplyNestedMultipartMimeMessage);
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 4);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailAttachments.REL_ATTACHMENTS);
        final String expectedContentTransferEncodingKey = ExtractEmailAttachments.ATTACHMENT_HEADER_ATTRIBUTE_PREFIX + "content-transfer-encoding";

        final List<Map<String, String>> expectedHeaderAttachmentAttributes = List.of(
                Map.of(expectedContentTransferEncodingKey, "quoted-printable", EXPECTED_CONTENT_TYPE_KEY, "text/plain; charset=iso-8859-1"),
                Map.of(expectedContentTransferEncodingKey, "quoted-printable", EXPECTED_CONTENT_TYPE_KEY, "text/html; charset=iso-8859-1"),
                Map.of(EXPECTED_CONTENT_DISPOSITION_KEY, "inline; filename=\"inline_image.png\"", expectedContentTransferEncodingKey, "base64",
                        EXPECTED_CONTENT_TYPE_KEY, "image/png; name=inline_image.png",
                        ExtractEmailAttachments.ATTACHMENT_HEADER_ATTRIBUTE_PREFIX + "content-id", "<0011223344556677@8899AABBCCDDEEFF>"),
                Map.of(EXPECTED_CONTENT_DISPOSITION_KEY, getPdfContentDisposition(), expectedContentTransferEncodingKey, "base64", EXPECTED_CONTENT_TYPE_KEY, "application/pdf; name=my-attachment.pdf",
                        ExtractEmailAttachments.ATTACHMENT_HEADER_ATTRIBUTE_PREFIX + "content-description", "my-attachment.pdf")
        );

        for (int index = 0; index < splits.size(); index++) {
            MockFlowFile split = splits.get(index);
            assertAttachmentHeaderAttributes(split, expectedHeaderAttachmentAttributes.get(index));
        }
    }

    private byte[] generateDeeplyNestedMultipartMimeMessage() throws Exception {
        final Properties props = new Properties();
        final Session session = Session.getDefaultInstance(props, null);
        final MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress("sender@example.com"));
        message.addRecipient(MimeMessage.RecipientType.TO, new InternetAddress("receiver@example.com"));
        message.setSubject("Deeply Nested Multipart Test");

        final MimeMultipart mixedMultipart = new MimeMultipart("mixed");
        final MimeMultipart relatedMultipart = new MimeMultipart("related");
        relatedMultipart.setSubType("related; type=\"multipart/alternative\"");
        final MimeMultipart alternativeMultipart = new MimeMultipart("alternative");

        final MimeBodyPart plainTextPart = new MimeBodyPart();
        plainTextPart.setContent("Hello World! This is plain text body.", "text/plain; charset=iso-8859-1");
        plainTextPart.setHeader("Content-Transfer-Encoding", "quoted-printable");
        alternativeMultipart.addBodyPart(plainTextPart);

        final MimeBodyPart htmlTextPart = new MimeBodyPart();
        htmlTextPart.setContent("<html><body><h1>Hello World!</h1> This is HTML body.</body></html>", "text/html; charset=iso-8859-1");
        htmlTextPart.setHeader("Content-Transfer-Encoding", "quoted-printable");
        alternativeMultipart.addBodyPart(htmlTextPart);

        final MimeBodyPart alternativeWrapperPart = new MimeBodyPart();
        alternativeWrapperPart.setContent(alternativeMultipart);
        relatedMultipart.addBodyPart(alternativeWrapperPart);

        final MimeBodyPart inlineImagePart = new MimeBodyPart();
        inlineImagePart.setContent("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=", "image/png; name=\"inline_image.png\"");
        inlineImagePart.setDisposition("inline; filename=\"inline_image.png\"");
        inlineImagePart.setHeader("Content-ID", "<0011223344556677@8899AABBCCDDEEFF>");
        inlineImagePart.setHeader("Content-Transfer-Encoding", "base64");
        relatedMultipart.addBodyPart(inlineImagePart);

        final MimeBodyPart relatedWrapperPart = new MimeBodyPart();
        relatedWrapperPart.setContent(relatedMultipart);
        mixedMultipart.addBodyPart(relatedWrapperPart);

        final MimeBodyPart pdfAttachmentPart = new MimeBodyPart();
        pdfAttachmentPart.setContent("JVBERi0xLjQKJdPr6gkwChMKMSAwIG9iagogIDw8IC9UeXBlIC9DYXRhbG9n...", "application/pdf; name=\"my-attachment.pdf\"");
        pdfAttachmentPart.setDescription("my-attachment.pdf");
        pdfAttachmentPart.setDisposition("attachment; filename=\"my-attachment.pdf\"");
        pdfAttachmentPart.setHeader("Content-Transfer-Encoding", "base64");
        pdfAttachmentPart.setHeader("Content-Disposition", getPdfContentDisposition());
        mixedMultipart.addBodyPart(pdfAttachmentPart);
        message.setContent(mixedMultipart);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        message.writeTo(outputStream);

        return outputStream.toByteArray();
    }

    private String getPdfContentDisposition() {
        return """
                attachment;
                    filename="my-attachment.pdf"; size=71521;
                    creation-date="Thu, 13 Aug 2026 11:02:50 GMT";
                    modification-date="Thu, 13 Aug 2026 11:01:24 GMT\"""";
    }

    private void assertAttachmentHeaderAttributes(MockFlowFile split, Map<String, String> expected) {
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            // Must account for jakarta.mail.internet.MimeBodyPart writing MIME headers using canonical CRLF (\r\n)
            split.assertAttributeEquals(entry.getKey(), entry.getValue().replace("\n", "\r\n"));
        }
    }
}
