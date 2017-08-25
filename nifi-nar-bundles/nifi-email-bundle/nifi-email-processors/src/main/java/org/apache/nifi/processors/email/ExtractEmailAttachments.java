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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Date;

import javax.activation.DataSource;
import javax.mail.Address;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.util.MimeMessageParser;


import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.stream.io.BufferedInputStream;



@SupportsBatching
@EventDriven
@SideEffectFree
@Tags({"split", "email"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Extract attachments from a mime formatted email file, splitting them into individual flowfiles.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename ", description = "The filename of the attachment"),
        @WritesAttribute(attribute = "email.attachment.parent.filename ", description = "The filename of the parent FlowFile"),
        @WritesAttribute(attribute = "email.attachment.parent.uuid", description = "The UUID of the original FlowFile."),
        @WritesAttribute(attribute = "mime.type", description = "The mime type of the attachment.")})

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
            .description("Flowfiles that could not be parsed")
            .build();
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ATTACHMENTS);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();

        this.descriptors = Collections.unmodifiableList(descriptors);
    }

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

        session.read(originalFlowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream rawIn) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        Properties props = new Properties();
                        props.put("mail.mime.address.strict", requireStrictAddresses);
                        Session mailSession = Session.getInstance(props);
                        MimeMessage originalMessage = new MimeMessage(mailSession, in);
                        MimeMessageParser parser = new MimeMessageParser(originalMessage).parse();
                        // RFC-2822 determines that a message must have a "From:" header
                        // if a message lacks the field, it is flagged as invalid
                        Address[] from = originalMessage.getFrom();
                        if (from == null) {
                            throw new MessagingException("Message failed RFC-2822 validation: No Sender");
                        }
                        Date sentDate = originalMessage.getSentDate();
                        if (sentDate == null) {
                            // Throws MessageException due to lack of minimum required headers
                            throw new MessagingException("Message failed RFC2822 validation: No Sent Date");
                        }
                        originalFlowFilesList.add(originalFlowFile);
                        if (parser.hasAttachments()) {
                            final String originalFlowFileName = originalFlowFile.getAttribute(CoreAttributes.FILENAME.key());
                            try {
                                for (final DataSource data : parser.getAttachmentList()) {
                                    FlowFile split = session.create(originalFlowFile);
                                    final Map<String, String> attributes = new HashMap<>();
                                    if (StringUtils.isNotBlank(data.getName())) {
                                        attributes.put(CoreAttributes.FILENAME.key(), data.getName());
                                    }
                                    if (StringUtils.isNotBlank(data.getContentType())) {
                                        attributes.put(CoreAttributes.MIME_TYPE.key(), data.getContentType());
                                    }
                                    String parentUuid = originalFlowFile.getAttribute(CoreAttributes.UUID.key());
                                    attributes.put(ATTACHMENT_ORIGINAL_UUID, parentUuid);
                                    attributes.put(ATTACHMENT_ORIGINAL_FILENAME, originalFlowFileName);
                                    split = session.append(split, new OutputStreamCallback() {
                                        @Override
                                        public void process(OutputStream out) throws IOException {
                                            IOUtils.copy(data.getInputStream(), out);
                                        }
                                    });
                                    split = session.putAllAttributes(split, attributes);
                                    attachmentsList.add(split);
                                }
                            } catch (FlowFileHandlingException e) {
                                // Something went wrong
                                // Removing splits that may have been created
                                session.remove(attachmentsList);
                                // Removing the original flow from its list
                                originalFlowFilesList.remove(originalFlowFile);
                                logger.error("Flowfile {} triggered error {} while processing message removing generated FlowFiles from sessions", new Object[]{originalFlowFile, e});
                                invalidFlowFilesList.add(originalFlowFile);
                            }
                        }
                    } catch (Exception e) {
                        // Another error hit...
                        // Removing the original flow from its list
                        originalFlowFilesList.remove(originalFlowFile);
                        logger.error("Could not parse the flowfile {} as an email, treating as failure", new Object[]{originalFlowFile, e});
                        // Message is invalid or triggered an error during parsing
                        invalidFlowFilesList.add(originalFlowFile);
                    }
                }
        });

        session.transfer(attachmentsList, REL_ATTACHMENTS);

        // As per above code, originalFlowfile may be routed to invalid or
        // original depending on RFC2822 compliance.
        session.transfer(invalidFlowFilesList, REL_FAILURE);
        session.transfer(originalFlowFilesList, REL_ORIGINAL);

        if (attachmentsList.size() > 10) {
            logger.info("Split {} into {} files", new Object[]{originalFlowFile, attachmentsList.size()});
        } else if (attachmentsList.size() > 1){
            logger.info("Split {} into {} files: {}", new Object[]{originalFlowFile, attachmentsList.size(), attachmentsList});
        }
     }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }


}
