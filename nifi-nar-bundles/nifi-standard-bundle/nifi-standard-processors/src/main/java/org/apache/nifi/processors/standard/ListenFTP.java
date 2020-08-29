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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.ftp.NifiFtpServer;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"ingest", "ftp", "listen"})
@CapabilityDescription("Starts an FTP Server and listens on a given port to transform incoming files into FlowFiles. "
        + "The URI of the Service will be ftp://{hostname}:{port}. The default port is 2221.")
public class ListenFTP extends AbstractSessionFactoryProcessor {

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for successfully received files")
            .build();

    public static final PropertyDescriptor BIND_ADDRESS = new PropertyDescriptor.Builder()
            .name("bind-address")
            .displayName("Bind Address")
            .description("The address the FTP server should be bound to. If not provided, the server binds to all available addresses.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("listening-port")
            .displayName("Listening Port")
            .description("The Port to listen on for incoming connections. On Linux, root privileges are required to use port numbers below 1024.")
            .required(true)
            .defaultValue("2221")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .displayName("Username")
            .description("The name of the user that is allowed to log in to the FTP server. " +
                    "If a username is provided, a password must also be provided. " +
                    "If no username is specified, anonymous connections will be permitted.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("If a Username is specified, then a password must also be specified. " +
                    "The password provided by the client trying to log in to the FTP server will be checked against this password.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            BIND_ADDRESS,
            PORT,
            USERNAME,
            PASSWORD
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Collections.singletonList(
            RELATIONSHIP_SUCCESS
    )));

    private volatile NifiFtpServer ftpServer;
    private volatile CountDownLatch sessionFactorySetSignal;
    private final AtomicReference<ProcessSessionFactory> sessionFactory = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void startFtpServer(ProcessContext context) {
        if (ftpServer == null) {
            sessionFactory.set(null);

            String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
            String bindAddress = context.getProperty(BIND_ADDRESS).evaluateAttributeExpressions().getValue();
            int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();

            try {
                sessionFactorySetSignal = new CountDownLatch(1);
                ftpServer = new NifiFtpServer.Builder()
                        .sessionFactory(sessionFactory)
                        .sessionFactorySetSignal(sessionFactorySetSignal)
                        .bindAddress(bindAddress)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();
                ftpServer.start();
            } catch (ProcessException processException) {
                getLogger().error(processException.getMessage(), processException);
                stopFtpServer();
                throw processException;
            }
        } else {
            getLogger().warn("Ftp server already started.");
        }
    }

    @OnStopped
    public void stopFtpServer() {
        if (ftpServer != null && !ftpServer.isStopped()) {
            ftpServer.stop();
        }
        ftpServer = null;
        sessionFactory.set(null);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.sessionFactory.compareAndSet(null, sessionFactory)) {
            sessionFactorySetSignal.countDown();
        }
        context.yield();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> results = new ArrayList<>(2);
        String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        if ((username == null) && (password != null)) {
            results.add(usernameOrPasswordIsNull(USERNAME, PASSWORD));
        } else if ((username != null) && (password == null)) {
            results.add(usernameOrPasswordIsNull(PASSWORD, USERNAME));
        } else if ((username != null) && (password != null)) {
            validateAgainstEmptyString(username, USERNAME, results);
            validateAgainstEmptyString(password, PASSWORD, results);
        }
        return results;
    }

    private ValidationResult usernameOrPasswordIsNull(PropertyDescriptor nullProperty, PropertyDescriptor nonNullProperty) {
        String explanation = String.format("'%s' and '%s' should either both be provided or none of them", nullProperty.getDisplayName(), nonNullProperty.getDisplayName());
        return createValidationResult(nullProperty.getDisplayName(), explanation);
    }

    private void validateAgainstEmptyString(String propertyValue, PropertyDescriptor property, Collection<ValidationResult> validationResults) {
        if (StringUtils.isBlank(propertyValue)) {
            if (propertyValue.isEmpty()) {
                validationResults.add(propertyIsEmptyString(property));
            } else {
                validationResults.add(propertyContainsOnlyWhitespace(property));
            }
        }
    }

    private ValidationResult propertyIsEmptyString(PropertyDescriptor property) {
        String explanation = String.format("'%s' cannot be an empty string", property.getDisplayName());
        return createValidationResult(property.getDisplayName(), explanation);
    }

    private ValidationResult propertyContainsOnlyWhitespace(PropertyDescriptor property) {
        String explanation = String.format("'%s' must contain at least one non-whitespace character", property.getDisplayName());
        return createValidationResult(property.getDisplayName(), explanation);
    }

    private ValidationResult createValidationResult(String subject, String explanation) {
        return new ValidationResult.Builder().subject(subject).valid(false).explanation(explanation).build();
    }

}
