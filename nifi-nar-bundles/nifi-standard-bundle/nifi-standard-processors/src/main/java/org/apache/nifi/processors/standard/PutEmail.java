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
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import javax.activation.DataHandler;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;
import javax.mail.internet.PreencodedMimeBodyPart;
import javax.mail.util.ByteArrayDataSource;

import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

@SupportsBatching
@Tags({"email", "put", "notify", "smtp"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends an e-mail to configured recipients for each incoming FlowFile")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The entirety of the FlowFile's content (as a String object) "
        + "will be read into memory in case the property to use the flow file content as the email body is set to true.")
public class PutEmail extends AbstractProcessor {

    public static final PropertyDescriptor SMTP_HOSTNAME = new PropertyDescriptor.Builder()
            .name("SMTP Hostname")
            .description("The hostname of the SMTP host")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
            .name("SMTP Port")
            .description("The Port used for SMTP communications")
            .required(true)
            .defaultValue("25")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor SMTP_USERNAME = new PropertyDescriptor.Builder()
            .name("SMTP Username")
            .description("Username for the SMTP account")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor SMTP_PASSWORD = new PropertyDescriptor.Builder()
            .name("SMTP Password")
            .description("Password for the SMTP account")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor SMTP_AUTH = new PropertyDescriptor.Builder()
            .name("SMTP Auth")
            .description("Flag indicating whether authentication should be used")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor SMTP_TLS = new PropertyDescriptor.Builder()
            .name("SMTP TLS")
            .description("Flag indicating whether TLS should be enabled")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor SMTP_SOCKET_FACTORY = new PropertyDescriptor.Builder()
            .name("SMTP Socket Factory")
            .description("Socket Factory to use for SMTP Connection")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("javax.net.ssl.SSLSocketFactory")
            .build();
    public static final PropertyDescriptor HEADER_XMAILER = new PropertyDescriptor.Builder()
            .name("SMTP X-Mailer Header")
            .description("X-Mailer used in the header of the outgoing email")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("NiFi")
            .build();
    public static final PropertyDescriptor ATTRIBUTE_NAME_REGEX = new PropertyDescriptor.Builder()
            .name("attribute-name-regex")
            .displayName("Attributes to Send as Headers (Regex)")
            .description("A Regular Expression that is matched against all FlowFile attribute names. "
                + "Any attribute whose name matches the regex will be added to the Email messages as a Header. "
                + "If not specified, no FlowFile attributes will be added as headers.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("Content Type")
            .description("Mime Type used to interpret the contents of the email, such as text/plain or text/html")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("text/plain")
            .build();
    public static final PropertyDescriptor FROM = new PropertyDescriptor.Builder()
            .name("From")
            .description("Specifies the Email address to use as the sender. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor TO = new PropertyDescriptor.Builder()
            .name("To")
            .description("The recipients to include in the To-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CC = new PropertyDescriptor.Builder()
            .name("CC")
            .description("The recipients to include in the CC-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor BCC = new PropertyDescriptor.Builder()
            .name("BCC")
            .description("The recipients to include in the BCC-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SUBJECT = new PropertyDescriptor.Builder()
            .name("Subject")
            .description("The email subject")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("Message from NiFi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor MESSAGE = new PropertyDescriptor.Builder()
            .name("Message")
            .description("The body of the email message")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ATTACH_FILE = new PropertyDescriptor.Builder()
            .name("Attach File")
            .description("Specifies whether or not the FlowFile content should be attached to the email")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor CONTENT_AS_MESSAGE = new PropertyDescriptor.Builder()
            .name("email-ff-content-as-message")
            .displayName("Flow file content as message")
            .description("Specifies whether or not the FlowFile content should be the message of the email. If true, the 'Message' property is ignored.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor INCLUDE_ALL_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include All Attributes In Message")
            .description("Specifies whether or not all FlowFile attributes should be recorded in the body of the email message")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully sent will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that fail to send will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    /**
     * Mapping of the mail properties to the NiFi PropertyDescriptors that will be evaluated at runtime
     */
    private static final Map<String, PropertyDescriptor> propertyToContext = new HashMap<>();

    static {
        propertyToContext.put("mail.smtp.host", SMTP_HOSTNAME);
        propertyToContext.put("mail.smtp.port", SMTP_PORT);
        propertyToContext.put("mail.smtp.socketFactory.port", SMTP_PORT);
        propertyToContext.put("mail.smtp.socketFactory.class", SMTP_SOCKET_FACTORY);
        propertyToContext.put("mail.smtp.auth", SMTP_AUTH);
        propertyToContext.put("mail.smtp.starttls.enable", SMTP_TLS);
        propertyToContext.put("mail.smtp.user", SMTP_USERNAME);
        propertyToContext.put("mail.smtp.password", SMTP_PASSWORD);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SMTP_HOSTNAME);
        properties.add(SMTP_PORT);
        properties.add(SMTP_USERNAME);
        properties.add(SMTP_PASSWORD);
        properties.add(SMTP_AUTH);
        properties.add(SMTP_TLS);
        properties.add(SMTP_SOCKET_FACTORY);
        properties.add(HEADER_XMAILER);
        properties.add(ATTRIBUTE_NAME_REGEX);
        properties.add(CONTENT_TYPE);
        properties.add(FROM);
        properties.add(TO);
        properties.add(CC);
        properties.add(BCC);
        properties.add(SUBJECT);
        properties.add(MESSAGE);
        properties.add(CONTENT_AS_MESSAGE);
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

    private volatile Pattern attributeNamePattern = null;
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String attributeNameRegex = context.getProperty(ATTRIBUTE_NAME_REGEX).getValue();
        this.attributeNamePattern = attributeNameRegex == null ? null : Pattern.compile(attributeNameRegex);
    }

    private void setMessageHeader(final String header, final String value, final Message message) throws MessagingException {
        final ComponentLog logger = getLogger();
        try {
            message.setHeader(header, MimeUtility.encodeText(value));
        } catch (UnsupportedEncodingException e){
            logger.warn("Unable to add header {} with value {} due to encoding exception", new Object[]{header, value});
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Properties properties = this.getMailPropertiesFromFlowFile(context, flowFile);

        final Session mailSession = this.createMailSession(properties);

        final Message message = new MimeMessage(mailSession);
        final ComponentLog logger = getLogger();

        try {
            message.addFrom(toInetAddresses(context, flowFile, FROM));
            message.setRecipients(RecipientType.TO, toInetAddresses(context, flowFile, TO));
            message.setRecipients(RecipientType.CC, toInetAddresses(context, flowFile, CC));
            message.setRecipients(RecipientType.BCC, toInetAddresses(context, flowFile, BCC));

            if (attributeNamePattern != null) {
                for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
                    if (attributeNamePattern.matcher(entry.getKey()).matches()) {
                        this.setMessageHeader(entry.getKey(), entry.getValue(), message);
                    }
                }
            }
            this.setMessageHeader("X-Mailer", context.getProperty(HEADER_XMAILER).evaluateAttributeExpressions(flowFile).getValue(), message);
            message.setSubject(context.getProperty(SUBJECT).evaluateAttributeExpressions(flowFile).getValue());

            String messageText = getMessage(flowFile, context, session);

            String contentType = context.getProperty(CONTENT_TYPE).evaluateAttributeExpressions(flowFile).getValue();
            message.setContent(messageText, contentType);
            message.setSentDate(new Date());

            if (context.getProperty(ATTACH_FILE).asBoolean()) {
                final MimeBodyPart mimeText = new PreencodedMimeBodyPart("base64");
                mimeText.setDataHandler(new DataHandler(new ByteArrayDataSource(
                        Base64.encodeBase64(messageText.getBytes("UTF-8")), contentType + "; charset=\"utf-8\"")));
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

            send(message);

            session.getProvenanceReporter().send(flowFile, "mailto:" + message.getAllRecipients()[0].toString());
            session.transfer(flowFile, REL_SUCCESS);
            logger.info("Sent email as a result of receiving {}", new Object[]{flowFile});
        } catch (final ProcessException | MessagingException | IOException e) {
            context.yield();
            logger.error("Failed to send email for {}: {}; routing to failure", new Object[]{flowFile, e.getMessage()}, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String getMessage(final FlowFile flowFile, final ProcessContext context, final ProcessSession session) {
        String messageText = "";

        if(context.getProperty(CONTENT_AS_MESSAGE).evaluateAttributeExpressions(flowFile).asBoolean()) {
            // reading all the content of the input flow file
            final byte[] byteBuffer = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, byteBuffer, false);
                }
            });

            messageText = new String(byteBuffer, 0, byteBuffer.length, Charset.forName("UTF-8"));
        } else if (context.getProperty(MESSAGE).isSet()) {
            messageText = context.getProperty(MESSAGE).evaluateAttributeExpressions(flowFile).getValue();
        }

        if (context.getProperty(INCLUDE_ALL_ATTRIBUTES).asBoolean()) {
            return formatAttributes(flowFile, messageText);
        }

        return messageText;
    }

    /**
     * Based on the input properties, determine whether an authenticate or unauthenticated session should be used. If authenticated, creates a Password Authenticator for use in sending the email.
     *
     * @param properties mail properties
     * @return session
     */
    private Session createMailSession(final Properties properties) {
        String authValue = properties.getProperty("mail.smtp.auth");
        Boolean auth = Boolean.valueOf(authValue);

        /*
         * Conditionally create a password authenticator if the 'auth' parameter is set.
         */
        final Session mailSession = auth ? Session.getInstance(properties, new Authenticator() {
            @Override
            public PasswordAuthentication getPasswordAuthentication() {
                String username = properties.getProperty("mail.smtp.user"), password = properties.getProperty("mail.smtp.password");
                return new PasswordAuthentication(username, password);
            }
        }) : Session.getInstance(properties); // without auth
        return mailSession;
    }

    /**
     * Uses the mapping of javax.mail properties to NiFi PropertyDescriptors to build the required Properties object to be used for sending this email
     *
     * @param context context
     * @param flowFile flowFile
     * @return mail properties
     */
    private Properties getMailPropertiesFromFlowFile(final ProcessContext context, final FlowFile flowFile) {

        final Properties properties = new Properties();

        final ComponentLog logger = this.getLogger();

        for (Entry<String, PropertyDescriptor> entry : propertyToContext.entrySet()) {

            // Evaluate the property descriptor against the flow file
            String flowFileValue = context.getProperty(entry.getValue()).evaluateAttributeExpressions(flowFile).getValue();

            String property = entry.getKey();

            logger.debug("Evaluated Mail Property: {} with Value: {}", new Object[]{property, flowFileValue});

            // Nullable values are not allowed, so filter out
            if (null != flowFileValue) {
                properties.setProperty(property, flowFileValue);
            }

        }

        return properties;

    }

    public static final String BODY_SEPARATOR = "\n\n--------------------------------------------------\n";

    private static String formatAttributes(final FlowFile flowFile, final String messagePrepend) {
        StringBuilder message = new StringBuilder(messagePrepend);
        message.append(BODY_SEPARATOR);
        message.append("\nStandard FlowFile Metadata:");
        message.append(String.format("\n\t%1$s = '%2$s'", "id", flowFile.getAttribute(CoreAttributes.UUID.key())));
        message.append(String.format("\n\t%1$s = '%2$s'", "entryDate", new Date(flowFile.getEntryDate())));
        message.append(String.format("\n\t%1$s = '%2$s'", "fileSize", flowFile.getSize()));
        message.append("\nFlowFile Attributes:");
        for (Entry<String, String> attribute : flowFile.getAttributes().entrySet()) {
            message.append(String.format("\n\t%1$s = '%2$s'", attribute.getKey(), attribute.getValue()));
        }
        message.append("\n");
        return message.toString();
    }

    /**
     * @param context the current context
     * @param flowFile the current flow file
     * @param propertyDescriptor the property to evaluate
     * @return an InternetAddress[] parsed from the supplied property
     * @throws AddressException if the property cannot be parsed to a valid InternetAddress[]
     */
    private InternetAddress[] toInetAddresses(final ProcessContext context, final FlowFile flowFile,
            PropertyDescriptor propertyDescriptor) throws AddressException {
        InternetAddress[] parse;
        String value = context.getProperty(propertyDescriptor).evaluateAttributeExpressions(flowFile).getValue();
        if (value == null || value.isEmpty()){
            if (propertyDescriptor.isRequired()) {
                final String exceptionMsg = "Required property '" + propertyDescriptor.getDisplayName() + "' evaluates to an empty string.";
                throw new AddressException(exceptionMsg);
            } else {
                parse = new InternetAddress[0];
            }
        } else {
            try {
                parse = InternetAddress.parse(value);
            } catch (AddressException e) {
                final String exceptionMsg = "Unable to parse a valid address for property '" + propertyDescriptor.getDisplayName() + "' with value '"+ value +"'";
                throw new AddressException(exceptionMsg);
            }
        }
        return parse;
    }

    /**
     * Wrapper for static method {@link Transport#send(Message)} to add testability of this class.
     *
     * @param msg the message to send
     * @throws MessagingException on error
     */
    protected void send(final Message msg) throws MessagingException {
        Transport.send(msg);
    }

}
