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

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.activation.DataHandler;
import javax.mail.Message;
import javax.mail.Message.RecipientType;
import javax.mail.Session;
import javax.mail.URLName;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.PreencodedMimeBodyPart;
import javax.mail.util.ByteArrayDataSource;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.commons.codec.binary.Base64;

import com.sun.mail.smtp.SMTPTransport;

@SupportsBatching
@Tags({"email", "put", "notify", "smtp"})
@CapabilityDescription("Sends an e-mail to configured recipients for each incoming FlowFile")
public class PutEmail extends AbstractProcessor {

    public static final PropertyDescriptor SMTP_HOSTNAME = new PropertyDescriptor.Builder()
            .name("SMTP Hostname")
            .description("The hostname of the SMTP host")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
            .name("SMTP Port")
            .description("The Port used for SMTP communications")
            .required(true)
            .defaultValue("25")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor FROM = new PropertyDescriptor.Builder()
            .name("From")
            .description("Specifies the Email address to use as the sender")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor TO = new PropertyDescriptor.Builder()
            .name("To")
            .description("The recipients to include in the To-Line of the email")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CC = new PropertyDescriptor.Builder()
            .name("CC")
            .description("The recipients to include in the CC-Line of the email")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor BCC = new PropertyDescriptor.Builder()
            .name("BCC")
            .description("The recipients to include in the BCC-Line of the email")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SUBJECT = new PropertyDescriptor.Builder()
            .name("Subject")
            .description("The email subject")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("Message from NiFi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor MESSAGE = new PropertyDescriptor.Builder()
            .name("Message")
            .description("The body of the email message")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ATTACH_FILE = new PropertyDescriptor.Builder()
            .name("Attach File")
            .description("Specifies whether or not the FlowFile content should be attached to the email")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor INCLUDE_ALL_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include All Attributes In Message")
            .description("Specifies whether or not all FlowFile attributes should be recorded in the body of the email message")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("FlowFiles that are successfully sent will be routed to this relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles that fail to send will be routed to this relationship").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SMTP_HOSTNAME);
        properties.add(SMTP_PORT);
        properties.add(FROM);
        properties.add(TO);
        properties.add(CC);
        properties.add(BCC);
        properties.add(SUBJECT);
        properties.add(MESSAGE);
        properties.add(ATTACH_FILE);
        properties.add(INCLUDE_ALL_ATTRIBUTES);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(context));

        final String to = context.getProperty(TO).getValue();
        final String cc = context.getProperty(CC).getValue();
        final String bcc = context.getProperty(BCC).getValue();

        if (to == null && cc == null && bcc == null) {
            errors.add(new ValidationResult.Builder().subject("To, CC, BCC").valid(false).explanation("Must specify at least one To/CC/BCC address").build());
        }

        return errors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Properties properties = new Properties();
        properties.setProperty("smtp.mail.host", context.getProperty(SMTP_HOSTNAME).getValue());
        final Session mailSession = Session.getInstance(properties);
        final Message message = new MimeMessage(mailSession);
        final ProcessorLog logger = getLogger();

        try {
            message.setFrom(InternetAddress.parse(context.getProperty(FROM).evaluateAttributeExpressions(flowFile).getValue())[0]);

            final InternetAddress[] toAddresses = toInetAddresses(context.getProperty(TO).evaluateAttributeExpressions(flowFile).getValue());
            message.setRecipients(RecipientType.TO, toAddresses);

            final InternetAddress[] ccAddresses = toInetAddresses(context.getProperty(CC).evaluateAttributeExpressions(flowFile).getValue());
            message.setRecipients(RecipientType.CC, ccAddresses);

            final InternetAddress[] bccAddresses = toInetAddresses(context.getProperty(BCC).evaluateAttributeExpressions(flowFile).getValue());
            message.setRecipients(RecipientType.BCC, bccAddresses);

            message.setHeader("X-Mailer", "NiFi");
            message.setSubject(context.getProperty(SUBJECT).evaluateAttributeExpressions(flowFile).getValue());
            String messageText = context.getProperty(MESSAGE).evaluateAttributeExpressions(flowFile).getValue();

            if (context.getProperty(INCLUDE_ALL_ATTRIBUTES).asBoolean()) {
                messageText = formatAttributes(flowFile, messageText);
            }

            message.setText(messageText);
            message.setSentDate(new Date());

            if (context.getProperty(ATTACH_FILE).asBoolean()) {
                final MimeBodyPart mimeText = new PreencodedMimeBodyPart("base64");
                mimeText.setDataHandler(new DataHandler(new ByteArrayDataSource(Base64.encodeBase64(messageText.getBytes("UTF-8")), "text/plain; charset=\"utf-8\"")));
                final MimeBodyPart mimeFile = new MimeBodyPart();
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream stream) throws IOException {
                        try {
                            mimeFile.setDataHandler(new DataHandler(new ByteArrayDataSource(stream, "application/octet-stream")));
                        } catch (final Exception e) {
                            throw new IOException(e);
                        }
                    }
                });

                mimeFile.setFileName(flowFile.getAttribute(CoreAttributes.FILENAME.key()));
                MimeMultipart multipart = new MimeMultipart();
                multipart.addBodyPart(mimeText);
                multipart.addBodyPart(mimeFile);
                message.setContent(multipart);
            }

            final String smtpHost = context.getProperty(SMTP_HOSTNAME).getValue();
            final SMTPTransport transport = new SMTPTransport(mailSession, new URLName(smtpHost));
            try {
                final int smtpPort = context.getProperty(SMTP_PORT).asInteger();
                transport.connect(new Socket(smtpHost, smtpPort));
                transport.sendMessage(message, message.getAllRecipients());
            } finally {
                transport.close();
            }

            session.getProvenanceReporter().send(flowFile, "mailto:" + message.getAllRecipients()[0].toString());
            session.transfer(flowFile, REL_SUCCESS);
            logger.info("Sent email as a result of receiving {}", new Object[]{flowFile});
        } catch (final Exception e) {
            context.yield();
            logger.error("Failed to send email for {}: {}; routing to failure", new Object[]{flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    public static final String BODY_SEPARATOR = "\n\n--------------------------------------------------\n";

    private static String formatAttributes(final FlowFile flowFile, final String messagePrepend) {
        StringBuilder message = new StringBuilder(messagePrepend);
        message.append(BODY_SEPARATOR);
        message.append("\nStandard FlowFile Metadata:");
        message.append(String.format("\n\t%1$s = '%2$s'", "id", flowFile.getId()));
        message.append(String.format("\n\t%1$s = '%2$s'", "entryDate", new Date(flowFile.getEntryDate())));
        message.append(String.format("\n\t%1$s = '%2$s'", "fileSize", flowFile.getSize()));
        message.append("\nFlowFile Attributes:");
        for (Entry<String, String> attribute : flowFile.getAttributes().entrySet()) {
            message.append(String.format("\n\t%1$s = '%2$s'", attribute.getKey(), attribute.getValue()));
        }
        message.append("\n");
        return message.toString();
    }

    private static InternetAddress[] toInetAddresses(final String val) throws AddressException {
        if (val == null) {
            return new InternetAddress[0];
        }
        return InternetAddress.parse(val);
    }

}
