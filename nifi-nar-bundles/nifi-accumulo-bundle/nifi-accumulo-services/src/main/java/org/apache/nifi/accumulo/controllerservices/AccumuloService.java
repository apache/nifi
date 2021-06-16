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
package org.apache.nifi.accumulo.controllerservices;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Purpose: Controller service that provides us a configured connector. Note that we don't need to close this
 *
 * Justification: Centralizes the configuration of the connecting accumulo code. This also will be used
 * for any kerberos integration.
 */
@RequiresInstanceClassLoading
@Tags({"accumulo", "client", "service"})
@CapabilityDescription("A controller service for accessing an HBase client.")
public class AccumuloService extends AbstractControllerService implements BaseAccumuloService {

    private enum AuthenticationType{
        PASSWORD,
        NONE
    }

    protected static final PropertyDescriptor ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
            .name("ZooKeeper Quorum")
            .description("Comma-separated list of ZooKeeper hosts for Accumulo.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor INSTANCE_NAME = new PropertyDescriptor.Builder()
            .name("Instance Name")
            .description("Instance name of the Accumulo cluster")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    protected static final PropertyDescriptor ACCUMULO_USER = new PropertyDescriptor.Builder()
            .name("Accumulo User")
            .description("Connecting user for Accumulo")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor ACCUMULO_PASSWORD = new PropertyDescriptor.Builder()
            .name("Accumulo Password")
            .description("Connecting user's password when using the PASSWORD Authentication type")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
            .name("Authentication Type")
            .description("Authentication Type")
            .allowableValues(AuthenticationType.values())
            .defaultValue(AuthenticationType.PASSWORD.toString())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    /**
     * Reference to the accumulo client.
     */
    AccumuloClient client;

    /**
     * properties
     */
    private List<PropertyDescriptor> properties;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ZOOKEEPER_QUORUM);
        props.add(INSTANCE_NAME);
        props.add(ACCUMULO_USER);
        props.add(AUTHENTICATION_TYPE);
        props.add(ACCUMULO_PASSWORD);
        properties = Collections.unmodifiableList(props);
    }

    private AuthenticationToken getToken(final AuthenticationType type, final ConfigurationContext context){
        switch(type){
            case PASSWORD:
                return new PasswordToken(context.getProperty(ACCUMULO_PASSWORD).getValue());
            default:
                return null;
        }
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(INSTANCE_NAME);
        properties.add(ZOOKEEPER_QUORUM);
        properties.add(ACCUMULO_USER);
        properties.add(AUTHENTICATION_TYPE);
        properties.add(ACCUMULO_PASSWORD);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>();

        if (!validationContext.getProperty(INSTANCE_NAME).isSet()){
            problems.add(new ValidationResult.Builder().valid(false).subject(INSTANCE_NAME.getName()).explanation("Instance name must be supplied").build());
        }

        if (!validationContext.getProperty(ZOOKEEPER_QUORUM).isSet()){
            problems.add(new ValidationResult.Builder().valid(false).subject(ZOOKEEPER_QUORUM.getName()).explanation("Zookeepers must be supplied").build());
        }

        if (!validationContext.getProperty(ACCUMULO_USER).isSet()){
            problems.add(new ValidationResult.Builder().valid(false).subject(ACCUMULO_USER.getName()).explanation("Accumulo user must be supplied").build());
        }

        final AuthenticationType type = validationContext.getProperty(
                AUTHENTICATION_TYPE).isSet() ? AuthenticationType.valueOf( validationContext.getProperty(AUTHENTICATION_TYPE).getValue() ) : AuthenticationType.PASSWORD;

        switch(type){
            case PASSWORD:
                if (!validationContext.getProperty(ACCUMULO_PASSWORD).isSet()){
                    problems.add(
                            new ValidationResult.Builder().valid(false).subject(AUTHENTICATION_TYPE.getName()).explanation("Password must be supplied for the Password Authentication type").build());
                }
                break;
            default:
                problems.add(new ValidationResult.Builder().valid(false).subject(ACCUMULO_PASSWORD.getName()).explanation("Non supported Authentication type").build());
        }

        return problems;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, InterruptedException {
        if (!context.getProperty(INSTANCE_NAME).isSet() || !context.getProperty(ZOOKEEPER_QUORUM).isSet() || !context.getProperty(ACCUMULO_USER).isSet()){
            throw new InitializationException("Instance name and Zookeeper Quorum must be specified");
        }



        final String instanceName = context.getProperty(INSTANCE_NAME).evaluateAttributeExpressions().getValue();
        final String zookeepers = context.getProperty(ZOOKEEPER_QUORUM).evaluateAttributeExpressions().getValue();
        final String accumuloUser = context.getProperty(ACCUMULO_USER).evaluateAttributeExpressions().getValue();

        final AuthenticationType type = AuthenticationType.valueOf( context.getProperty(AUTHENTICATION_TYPE).getValue() );



        final AuthenticationToken token = getToken(type,context);

        this.client = Accumulo.newClient().to(instanceName,zookeepers).as(accumuloUser,token).build();

        if (null == token){
            throw new InitializationException("Feature not implemented");
        }

    }

    @Override
    public AccumuloClient getClient(){
        return client;
    }

    @OnDisabled
    public void shutdown() {
        client.close();
    }

}
