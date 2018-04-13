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
package org.apache.nifi.bootstrap.notification.email;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.Message.RecipientType;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import org.apache.nifi.bootstrap.notification.AbstractNotificationService;
import org.apache.nifi.bootstrap.notification.NotificationContext;
import org.apache.nifi.bootstrap.notification.NotificationFailedException;
import org.apache.nifi.bootstrap.notification.NotificationType;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

public class EmailNotificationService extends AbstractNotificationService {

    public static final PropertyDescriptor SMTP_HOSTNAME = new PropertyDescriptor.Builder()
        .name("SMTP Hostname")
        .description("The hostname of the SMTP Server that is used to send Email Notifications")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .build();
    public static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
        .name("SMTP Port")
        .description("The Port used for SMTP communications")
        .required(true)
        .defaultValue("25")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .build();
    public static final PropertyDescriptor SMTP_USERNAME = new PropertyDescriptor.Builder()
        .name("SMTP Username")
        .description("Username for the SMTP account")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(false)
        .build();
    public static final PropertyDescriptor SMTP_PASSWORD = new PropertyDescriptor.Builder()
        .name("SMTP Password")
        .description("Password for the SMTP account")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(false)
        .sensitive(true)
        .build();
    public static final PropertyDescriptor SMTP_AUTH = new PropertyDescriptor.Builder()
        .name("SMTP Auth")
        .description("Flag indicating whether authentication should be used")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .build();
    public static final PropertyDescriptor SMTP_TLS = new PropertyDescriptor.Builder()
        .name("SMTP TLS")
        .description("Flag indicating whether TLS should be enabled")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build();
    public static final PropertyDescriptor SMTP_SOCKET_FACTORY = new PropertyDescriptor.Builder()
        .name("SMTP Socket Factory")
        .description("Socket Factory to use for SMTP Connection")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("javax.net.ssl.SSLSocketFactory")
        .build();
    public static final PropertyDescriptor HEADER_XMAILER = new PropertyDescriptor.Builder()
        .name("SMTP X-Mailer Header")
        .description("X-Mailer used in the header of the outgoing email")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("NiFi")
        .build();
    public static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor.Builder()
        .name("Content Type")
        .description("Mime Type used to interpret the contents of the email, such as text/plain or text/html")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("text/plain")
        .build();
    public static final PropertyDescriptor FROM = new PropertyDescriptor.Builder()
        .name("From")
        .description("Specifies the Email address to use as the sender")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor TO = new PropertyDescriptor.Builder()
        .name("To")
        .description("The recipients to include in the To-Line of the email")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor CC = new PropertyDescriptor.Builder()
        .name("CC")
        .description("The recipients to include in the CC-Line of the email")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor BCC = new PropertyDescriptor.Builder()
        .name("BCC")
        .description("The recipients to include in the BCC-Line of the email")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

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
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SMTP_HOSTNAME);
        properties.add(SMTP_PORT);
        properties.add(SMTP_USERNAME);
        properties.add(SMTP_PASSWORD);
        properties.add(SMTP_AUTH);
        properties.add(SMTP_TLS);
        properties.add(SMTP_SOCKET_FACTORY);
        properties.add(HEADER_XMAILER);
        properties.add(CONTENT_TYPE);
        properties.add(FROM);
        properties.add(TO);
        properties.add(CC);
        properties.add(BCC);
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
    public void notify(final NotificationContext context, final NotificationType notificationType, final String subject, final String messageText) throws NotificationFailedException {
        final Properties properties = getMailProperties(context);
        final Session mailSession = createMailSession(properties);
        final Message message = new MimeMessage(mailSession);

        try {
            message.setFrom(InternetAddress.parse(context.getProperty(FROM).evaluateAttributeExpressions().getValue())[0]);

            final InternetAddress[] toAddresses = toInetAddresses(context.getProperty(TO).evaluateAttributeExpressions().getValue());
            message.setRecipients(RecipientType.TO, toAddresses);

            final InternetAddress[] ccAddresses = toInetAddresses(context.getProperty(CC).evaluateAttributeExpressions().getValue());
            message.setRecipients(RecipientType.CC, ccAddresses);

            final InternetAddress[] bccAddresses = toInetAddresses(context.getProperty(BCC).evaluateAttributeExpressions().getValue());
            message.setRecipients(RecipientType.BCC, bccAddresses);

            message.setHeader("X-Mailer", context.getProperty(HEADER_XMAILER).evaluateAttributeExpressions().getValue());
            message.setSubject(subject);

            final String contentType = context.getProperty(CONTENT_TYPE).evaluateAttributeExpressions().getValue();
            message.setContent(messageText, contentType);
            message.setSentDate(new Date());

            Transport.send(message);
        } catch (final ProcessException | MessagingException e) {
            throw new NotificationFailedException("Failed to send E-mail Notification", e);
        }
    }

    /**
     * Creates an array of 0 or more InternetAddresses for the given String
     *
     * @param val the String to parse for InternetAddresses
     * @return an array of 0 or more InetAddresses
     * @throws AddressException if val contains an invalid address
     */
    private static InternetAddress[] toInetAddresses(final String val) throws AddressException {
        if (val == null) {
            return new InternetAddress[0];
        }
        return InternetAddress.parse(val);
    }

    /**
     * Uses the mapping of javax.mail properties to NiFi PropertyDescriptors to build the required Properties object to be used for sending this email
     *
     * @param context context
     * @return mail properties
     */
    private Properties getMailProperties(final NotificationContext context) {
        final Properties properties = new Properties();

        for (Entry<String, PropertyDescriptor> entry : propertyToContext.entrySet()) {
            // Evaluate the property descriptor against the flow file
            String property = entry.getKey();
            String propValue = context.getProperty(entry.getValue()).evaluateAttributeExpressions().getValue();

            // Nullable values are not allowed, so filter out
            if (null != propValue) {
                properties.setProperty(property, propValue);
            }
        }

        return properties;
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

}
