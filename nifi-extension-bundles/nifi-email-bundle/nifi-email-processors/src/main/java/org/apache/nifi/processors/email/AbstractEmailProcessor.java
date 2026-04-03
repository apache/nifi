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

import jakarta.mail.Address;
import jakarta.mail.FolderClosedException;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.StoreClosedException;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.mail.inbound.AbstractMailReceiver;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Base processor for implementing processors to consume messages from Email
 * servers using Spring Integration libraries.
 *
 * @param <T> the type of {@link AbstractMailReceiver}.
 */
abstract class AbstractEmailProcessor<T extends AbstractMailReceiver> extends AbstractProcessor {
    public static final AllowableValue PASSWORD_BASED_AUTHORIZATION_MODE = new AllowableValue(
            "password-based-authorization-mode",
            "Use Password",
            "Use password"
    );

    public static final AllowableValue OAUTH_AUTHORIZATION_MODE = new AllowableValue(
            "oauth-based-authorization-mode",
            "Use OAuth2",
            "Use OAuth2 to acquire access token"
    );
    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("Host Name")
            .description("Network address of Email server (e.g., pop.gmail.com, imap.gmail.com . . .)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("Numeric value identifying Port of Email server (e.g., 993)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor AUTHORIZATION_MODE = new PropertyDescriptor.Builder()
            .name("Authorization Mode")
            .description("How to authorize sending email on the user's behalf.")
            .required(true)
            .allowableValues(PASSWORD_BASED_AUTHORIZATION_MODE, OAUTH_AUTHORIZATION_MODE)
            .defaultValue(PASSWORD_BASED_AUTHORIZATION_MODE)
            .build();
    public static final PropertyDescriptor OAUTH2_ACCESS_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("OAuth2 Access Token Provider")
            .description("OAuth2 service that can provide access tokens.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .dependsOn(AUTHORIZATION_MODE, OAUTH_AUTHORIZATION_MODE)
            .required(true)
            .build();
    public static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("User Name")
            .description("User Name used for authentication and authorization with Email server.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password used for authentication and authorization with Email server.")
            .dependsOn(AUTHORIZATION_MODE, PASSWORD_BASED_AUTHORIZATION_MODE)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor FOLDER = new PropertyDescriptor.Builder()
            .name("Folder")
            .description("Email folder to retrieve messages from (e.g., INBOX)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("INBOX")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch Size")
            .description("Specify the maximum number of Messages to fetch per call to Email Server.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("10")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor SHOULD_DELETE_MESSAGES = new PropertyDescriptor.Builder()
            .name("Delete Messages")
            .description("Specify whether mail messages should be deleted after retrieval.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("The amount of time to wait to connect to Email server")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("30 sec")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All messages that are the are successfully received from Email server and converted to FlowFiles are routed to this relationship")
            .build();

    /*
     * Will ensure that list of PropertyDescriptors is build only once, since
     * all other lifecycle methods are invoked multiple times.
     */
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HOST,
            PORT,
            AUTHORIZATION_MODE,
            OAUTH2_ACCESS_TOKEN_PROVIDER,
            USER,
            PASSWORD,
            FOLDER,
            FETCH_SIZE,
            SHOULD_DELETE_MESSAGES,
            CONNECTION_TIMEOUT
    );

    static final Set<Relationship> SHARED_RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private ScheduledExecutorService scheduledExecutorService;

    protected volatile T messageReceiver;

    private volatile BlockingQueue<Message> messageQueue;

    private volatile String displayUrl;

    private volatile ProcessSession processSession;

    private volatile OAuth2AccessTokenProvider accessTokenProvider;
    private volatile AccessToken currentAccessToken;

    protected static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ThreadFactory threadFactory = Thread.ofPlatform()
                .name("%s[%s]-MailReceiver".formatted(getClass().getSimpleName(), getIdentifier()))
                .factory();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);

        if (context.getProperty(AUTHORIZATION_MODE).getValue().equals(OAUTH_AUTHORIZATION_MODE.getValue())) {
            OAuth2AccessTokenProvider oauth2AccessTokenProvider = context.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);

            accessTokenProvider = oauth2AccessTokenProvider;
            currentAccessToken = oauth2AccessTokenProvider.getAccessDetails();
        } else {
            accessTokenProvider = null;
            currentAccessToken = null;
        }
    }

    @OnStopped
    public void onStopped() {
        flushRemainingMessages();
        try {
            messageReceiver.destroy();
            messageReceiver = null;
        } catch (final Exception e) {
            getLogger().warn("Failed to close Mail Receiver", e);
        }

        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
            scheduledExecutorService = null;
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return SHARED_RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        initializeIfNecessary(context, session);

        final Message emailMessage = receiveMessage();
        if (emailMessage != null) {
            transfer(emailMessage, session);
        }
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty("host", HOST.getName());
        config.renameProperty("port", PORT.getName());
        config.renameProperty("authorization-mode", AUTHORIZATION_MODE.getName());
        config.renameProperty("oauth2-access-token-provider", OAUTH2_ACCESS_TOKEN_PROVIDER.getName());
        config.renameProperty("user", USER.getName());
        config.renameProperty("password", PASSWORD.getName());
        config.renameProperty("folder", FOLDER.getName());
        config.renameProperty("fetch.size", FETCH_SIZE.getName());
        config.renameProperty("delete.messages", SHOULD_DELETE_MESSAGES.getName());
        config.renameProperty("connection.timeout", CONNECTION_TIMEOUT.getName());
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Java Mail property.")
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true)
                .build();
    }

    /**
     * Delegates to sub-classes to build the target receiver as
     * {@link AbstractMailReceiver}
     *
     * @param context instance of {@link ProcessContext}
     * @return new instance of {@link AbstractMailReceiver}
     */
    protected abstract T buildMessageReceiver(ProcessContext context);

    /**
     * Return the target receiver's mail protocol (e.g., imap, pop etc.)
     */
    protected abstract String getProtocol(ProcessContext processContext);

    String buildUrl(final ProcessContext processContext) {
        String host = processContext.getProperty(HOST).evaluateAttributeExpressions().getValue();
        String port = processContext.getProperty(PORT).evaluateAttributeExpressions().getValue();
        String user = processContext.getProperty(USER).evaluateAttributeExpressions().getValue();

        final String password;
        if (accessTokenProvider == null) {
            password = processContext.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        } else {
            final AccessToken accessDetails = accessTokenProvider.getAccessDetails();
            password = accessDetails.getAccessToken();
        }

        String folder = processContext.getProperty(FOLDER).evaluateAttributeExpressions().getValue();

        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append(URLEncoder.encode(user, StandardCharsets.UTF_8));

        urlBuilder.append(":");
        urlBuilder.append(URLEncoder.encode(password, StandardCharsets.UTF_8));
        urlBuilder.append("@");
        urlBuilder.append(host);
        urlBuilder.append(":");
        urlBuilder.append(port);
        urlBuilder.append("/");
        urlBuilder.append(folder);

        String protocol = this.getProtocol(processContext);
        String finalUrl = protocol + "://" + urlBuilder;

        // build display-safe URL
        int passwordStartIndex = urlBuilder.indexOf(":") + 1;
        int passwordEndIndex = urlBuilder.indexOf("@");
        urlBuilder.replace(passwordStartIndex, passwordEndIndex, "[password]");
        this.displayUrl = protocol + "://" + urlBuilder;
        getLogger().info("Connecting to server [{}]", this.displayUrl);

        return finalUrl;
    }

    /**
     * Builds and initializes the target message receiver if necessary (if it's
     * null). Upon execution of this operation the receiver is fully functional
     * and is ready to receive messages.
     */
    private synchronized void initializeIfNecessary(ProcessContext context, ProcessSession processSession) {
        if (this.messageReceiver == null || isAccessTokenRefreshRequired()) {
            this.processSession = processSession;
            this.messageReceiver = this.buildMessageReceiver(context);

            int fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();

            this.messageReceiver.setMaxFetchSize(fetchSize);
            this.messageReceiver.setJavaMailProperties(this.buildJavaMailProperties(context));
            // Spring Integration 7 expects an evaluation context bean; register a lightweight one for the receiver
            final StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
            final StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
            evaluationContext.setBeanResolver(new BeanFactoryResolver(beanFactory));
            beanFactory.addBean(IntegrationContextUtils.INTEGRATION_EVALUATION_CONTEXT_BEAN_NAME, evaluationContext);
            beanFactory.addBean(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME, new ConcurrentTaskScheduler(scheduledExecutorService));
            this.messageReceiver.setBeanFactory(beanFactory);
            this.messageReceiver.afterPropertiesSet();

            this.messageQueue = new ArrayBlockingQueue<>(fetchSize);
        }
    }

    private boolean isAccessTokenRefreshRequired() {
        final boolean refreshRequired;

        if (accessTokenProvider == null) {
            refreshRequired = false;
        } else {
            refreshRequired = currentAccessToken == null || !currentAccessToken.equals(accessTokenProvider.getAccessDetails());
        }

        return refreshRequired;
    }

    /**
     * Extracts dynamic properties which typically represent the Java Mail
     * properties from the {@link ProcessContext} returning them as instance of
     * {@link Properties}
     */
    private Properties buildJavaMailProperties(ProcessContext context) {
        Properties javaMailProperties = new Properties();
        for (Entry<PropertyDescriptor, String> propertyDescriptorEntry : context.getProperties().entrySet()) {
            if (propertyDescriptorEntry.getKey().isDynamic()
                    && !propertyDescriptorEntry.getKey().getName().equals("mail.imap.timeout")
                    && !propertyDescriptorEntry.getKey().getName().equals("mail.pop3.timeout")) {
                javaMailProperties.setProperty(propertyDescriptorEntry.getKey().getName(),
                        propertyDescriptorEntry.getValue());
            }
        }
        String protocol = this.getProtocol(context);

        String propertyName = protocol.equals("pop3") ? "mail.pop3.timeout" : "mail.imap.timeout";
        final String timeoutInMillis = String.valueOf(context.getProperty(CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS));
        javaMailProperties.setProperty(propertyName, timeoutInMillis);

        if (accessTokenProvider != null) {
            final String authMechanismsProperty = "mail.%s.auth.mechanisms".formatted(protocol);
            javaMailProperties.put(authMechanismsProperty, "XOAUTH2");
        }

        return javaMailProperties;
    }

    /**
     * Fills the internal message queue if such queue is empty. This is due to
     * the fact that per single session there may be multiple messages retrieved
     * from the email server (see FETCH_SIZE).
     */
    private synchronized void fillMessageQueueIfNecessary() {
        if (this.messageQueue.isEmpty()) {
            Object[] messages;
            try {
                messages = this.messageReceiver.receive();
            } catch (final MessagingException e) {
                if (isClosedException(e)) {
                    // Destroy Receiver to force reinitialization on subsequent Processor.onTrigger()
                    messageReceiver.destroy();
                    messageReceiver = null;
                }
                throw new ProcessException("Failed to receive messages from server [%s]".formatted(displayUrl), e);
            }

            if (messages != null) {
                for (Object message : messages) {
                    this.messageQueue.offer((Message) message);
                }
            }
        }
    }

    private boolean isClosedException(final MessagingException exception) {
        final boolean closedException;

        final Exception nextException = exception.getNextException();
        if (exception instanceof FolderClosedException || exception instanceof StoreClosedException) {
            closedException = true;
        } else {
            // Handle IOException and subclasses as closed exceptions
            closedException = nextException instanceof IOException;
        }

        return closedException;
    }

    private void transfer(final Message message, final ProcessSession session) {
        final long started = System.nanoTime();
        FlowFile flowFile = session.create();

        flowFile = session.write(flowFile, out -> {
            try {
                message.writeTo(out);
            } catch (final MessagingException e) {
                throw new IOException("Message [%d] serialization failed".formatted(message.getMessageNumber()), e);
            }
        });

        final long executionDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        String fromAddresses = "";
        try {
            final Address[] from = message.getFrom();
            if (from != null) {
                fromAddresses = Arrays.asList(from).toString();
            }
        } catch (final MessagingException e) {
            getLogger().warn("Failed to retrieve [From] address from Message [{}]", message.getMessageNumber());
        }

        session.getProvenanceReporter().receive(flowFile, displayUrl, "Received message from " + fromAddresses, executionDuration);
        getLogger().info("Received {} from {} in {} millis", flowFile, fromAddresses, executionDuration);
        session.transfer(flowFile, REL_SUCCESS);

    }

    /**
     * Receives message from the internal queue filling up the queue if
     * necessary.
     */
    private Message receiveMessage() {
        Message emailMessage = null;
        try {
            this.fillMessageQueueIfNecessary();
            emailMessage = this.messageQueue.poll(1, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            getLogger().debug("Interrupted while receiving messages");
        }
        return emailMessage;
    }

    private void flushRemainingMessages() {
        Message emailMessage;
        try {
            while ((emailMessage = this.messageQueue.poll(1, TimeUnit.MILLISECONDS)) != null) {
                transfer(emailMessage, processSession);
                processSession.commitAsync();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            getLogger().debug("Interrupted while processing remaining messages");
        }
    }
}
