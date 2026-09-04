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

import jakarta.activation.DataSource;
import jakarta.mail.Address;
import jakarta.mail.BodyPart;
import jakarta.mail.Header;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.Session;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimePart;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileHandlingException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@SupportsBatching
@SideEffectFree
@Tags({"split", "email"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Extract attachments from a mime formatted email file, splitting them into individual FlowFiles.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename ", description = "The filename of the attachment"),
        @WritesAttribute(attribute = "email.attachment.parent.filename ", description = "The filename of the parent FlowFile"),
        @WritesAttribute(attribute = "email.attachment.parent.uuid", description = "The UUID of the original FlowFile."),
        @WritesAttribute(attribute = "mime.type", description = "The mime type of the attachment."),
        @WritesAttribute(attribute = ExtractEmailAttachments.ATTACHMENT_HEADER_ATTRIBUTE_PREFIX + "<attachment header name>", description = "Attachment header.")})

public class ExtractEmailAttachments extends AbstractProcessor {
    public static final String ATTACHMENT_ORIGINAL_FILENAME = "email.attachment.parent.filename";
    public static final String ATTACHMENT_ORIGINAL_UUID = "email.attachment.parent.uuid";

    public static final Relationship REL_ATTACHMENTS = new Relationship.Builder()
            .name("attachments")
            .description("Each individual attachment will be routed to the attachments relationship")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original file")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that could not be parsed")
            .build();

    static final String ATTACHMENT_HEADER_ATTRIBUTE_PREFIX = "email.attachment.header.";
    private static final String ATTACHMENT_DISPOSITION = "attachment";

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ATTACHMENTS,
            REL_ORIGINAL,
            REL_FAILURE);

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final ComponentLog logger = getLogger();
        final FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null) {
            return;
        }
        final List<FlowFile> attachmentsList = new ArrayList<>();
        final List<FlowFile> invalidFlowFilesList = new ArrayList<>();
        final List<FlowFile> originalFlowFilesList = new ArrayList<>();

        final String requireStrictAddresses = "false";

        session.read(originalFlowFile, rawIn -> {
            try (final InputStream in = new BufferedInputStream(rawIn)) {
                Properties props = new Properties();
                props.put("mail.mime.address.strict", requireStrictAddresses);
                Session mailSession = Session.getInstance(props);
                MimeMessage originalMessage = new MimeMessage(mailSession, in);

                // RFC-2822 determines that a message must have a "From:" header
                // if a message lacks the field, it is flagged as invalid
                Address[] from = originalMessage.getFrom();
                if (from == null) {
                    throw new MessagingException("Message failed RFC-2822 validation: No Sender");
                }
                originalFlowFilesList.add(originalFlowFile);

                final String originalFlowFileName = originalFlowFile.getAttribute(CoreAttributes.FILENAME.key());
                try {
                    final List<Attachment> attachments = new ArrayList<>();
                    parseAttachments(attachments, originalMessage, 0);

                    for (final Attachment attachment : attachments) {
                        FlowFile split = session.create(originalFlowFile);
                        final Map<String, String> attributes = new HashMap<>();
                        final DataSource data = attachment.dataSource();
                        final String name = data.getName();
                        if (name != null && !name.isBlank()) {
                            attributes.put(CoreAttributes.FILENAME.key(), name);
                        }
                        final String contentType = data.getContentType();
                        if (contentType != null && !contentType.isBlank()) {
                            attributes.put(CoreAttributes.MIME_TYPE.key(), contentType);
                        }

                        for (Map.Entry<String, String> entry : attachment.headers().entrySet()) {
                            final String headerAttributeName = ATTACHMENT_HEADER_ATTRIBUTE_PREFIX + entry.getKey();
                            final String headerAttributeValue = entry.getValue();
                            attributes.put(headerAttributeName, headerAttributeValue);
                        }

                        String parentUuid = originalFlowFile.getAttribute(CoreAttributes.UUID.key());
                        attributes.put(ATTACHMENT_ORIGINAL_UUID, parentUuid);
                        attributes.put(ATTACHMENT_ORIGINAL_FILENAME, originalFlowFileName);
                        split = session.append(split, out -> data.getInputStream().transferTo(out));
                        split = session.putAllAttributes(split, attributes);
                        attachmentsList.add(split);
                    }
                } catch (FlowFileHandlingException e) {
                    // Something went wrong
                    // Removing splits that may have been created
                    session.remove(attachmentsList);
                    // Removing the original flow from its list
                    originalFlowFilesList.remove(originalFlowFile);
                    logger.error("Flowfile {} triggered error {} while processing message removing generated FlowFiles from sessions", originalFlowFile, e);
                    invalidFlowFilesList.add(originalFlowFile);
                }
            } catch (Exception e) {
                // Another error hit...
                // Removing the original flow from its list
                originalFlowFilesList.remove(originalFlowFile);
                logger.error("Could not parse the flowfile {} as an email, treating as failure", originalFlowFile, e);
                // Message is invalid or triggered an error during parsing
                invalidFlowFilesList.add(originalFlowFile);
            }
        });

        session.transfer(attachmentsList, REL_ATTACHMENTS);

        // As per above code, originalFlowfile may be routed to invalid or
        // original depending on RFC2822 compliance.
        session.transfer(invalidFlowFilesList, REL_FAILURE);
        session.transfer(originalFlowFilesList, REL_ORIGINAL);

        if (attachmentsList.size() > 10) {
            logger.info("Split {} into {} files", originalFlowFile, attachmentsList.size());
        } else if (attachmentsList.size() > 1) {
            logger.info("Split {} into {} files: {}", originalFlowFile, attachmentsList.size(), attachmentsList);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private void parseAttachments(final List<Attachment> attachments, final MimePart parentPart, final int depth) throws MessagingException, IOException {
        final String disposition = parentPart.getDisposition();

        final Object parentContent = parentPart.getContent();
        if (parentContent instanceof final Multipart multipart) {
            final int count = multipart.getCount();
            final int partDepth = depth + 1;
            for (int i = 0; i < count; i++) {
                final BodyPart bodyPart = multipart.getBodyPart(i);
                if (bodyPart instanceof final MimeBodyPart mimeBodyPart) {
                    parseAttachments(attachments, mimeBodyPart, partDepth);
                }
            }
        } else if (ATTACHMENT_DISPOSITION.equalsIgnoreCase(disposition) || depth > 0) {
            final DataSource dataSource = parentPart.getDataHandler().getDataSource();
            final Map<String, String> extractedHeaders = new HashMap<>();

            if (parentPart instanceof final MimeBodyPart mimeBodyPart) {
                final Enumeration<Header> headers = mimeBodyPart.getAllHeaders();
                while (headers.hasMoreElements()) {
                    final Header header = headers.nextElement();
                    final String name = header.getName();
                    if (name != null && !name.isBlank()) {
                        final String value = header.getValue();
                        extractedHeaders.put(name, value);
                    }
                }
            }

            final Attachment attachment = new Attachment(dataSource, extractedHeaders);
            attachments.add(attachment);
        }
    }
}

record Attachment(DataSource dataSource, Map<String, String> headers) { }
