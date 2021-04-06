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
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Purpose: Controller service that provides us a configured connector. Note that we don't need to close this
 *
 * Justification: Centralizes the configuration of the connecting accumulo code. This also will be used
 * for any kerberos integration.
 */
@RequiresInstanceClassLoading
@Tags({"accumulo", "client", "service"})
@CapabilityDescription("A controller service for accessing an Accumulo Client.")
public class AccumuloService extends AbstractControllerService implements BaseAccumuloService {

    private enum AuthenticationType {
        PASSWORD,
        KERBEROS,
        NONE
    }

    protected static final PropertyDescriptor ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
            .name("ZooKeeper Quorum")
            .displayName("ZooKeeper Quorum")
            .description("Comma-separated list of ZooKeeper hosts for Accumulo.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor INSTANCE_NAME = new PropertyDescriptor.Builder()
            .name("Instance Name")
            .displayName("Instance Name")
            .description("Instance name of the Accumulo cluster")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
            .name("accumulo-authentication-type")
            .displayName("Authentication Type")
            .description("Authentication Type")
            .allowableValues(AuthenticationType.values())
            .defaultValue(AuthenticationType.PASSWORD.toString())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ACCUMULO_USER = new PropertyDescriptor.Builder()
            .name("Accumulo User")
            .displayName("Accumulo User")
            .description("Connecting user for Accumulo")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.PASSWORD.toString())
            .build();

    protected static final PropertyDescriptor ACCUMULO_PASSWORD = new PropertyDescriptor.Builder()
            .name("Accumulo Password")
            .displayName("Accumulo Password")
            .description("Connecting user's password")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.PASSWORD.toString())
            .build();

    protected static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for principal + keytab Kerberos authentication")
            .identifiesControllerService(KerberosCredentialsService.class)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.KERBEROS.toString())
            .build();

    protected static final PropertyDescriptor KERBEROS_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("kerberos-principal")
            .displayName("Kerberos Principal")
            .description("Kerberos Principal")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.KERBEROS.toString())
            .build();

    protected static final PropertyDescriptor KERBEROS_PASSWORD = new PropertyDescriptor.Builder()
            .name("kerberos-password")
            .displayName("Kerberos Password")
            .description("Kerberos Password")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.KERBEROS.toString())
            .build();

    protected static final PropertyDescriptor ACCUMULO_SASL_QOP = new PropertyDescriptor.Builder()
            .name("accumulo-sasl-qop")
            .displayName("Accumulo SASL quality of protection")
            .description("Accumulo SASL quality of protection for KERBEROS Authentication type")
            .allowableValues("auth", "auth-int", "auth-conf")
            .defaultValue("auth-conf")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.KERBEROS.toString())
            .build();

    /**
     * Reference to the accumulo client.
     */
    AccumuloClient client;

    /**
     * properties
     */
    private List<PropertyDescriptor> properties;

    private KerberosUser kerberosUser;

    private AuthenticationType authType;

    @Override
    protected void init(ControllerServiceInitializationContext config) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ZOOKEEPER_QUORUM);
        props.add(INSTANCE_NAME);
        props.add(AUTHENTICATION_TYPE);
        props.add(ACCUMULO_USER);
        props.add(ACCUMULO_PASSWORD);
        props.add(KERBEROS_CREDENTIALS_SERVICE);
        props.add(KERBEROS_PRINCIPAL);
        props.add(KERBEROS_PASSWORD);
        props.add(ACCUMULO_SASL_QOP);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
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

        final AuthenticationType type = validationContext.getProperty(
                AUTHENTICATION_TYPE).isSet() ? AuthenticationType.valueOf( validationContext.getProperty(AUTHENTICATION_TYPE).getValue() ) : AuthenticationType.NONE;

        switch(type){
            case PASSWORD:
                if (!validationContext.getProperty(ACCUMULO_USER).isSet()){
                    problems.add(
                            new ValidationResult.Builder().valid(false).subject(ACCUMULO_USER.getName()).explanation("Accumulo user must be supplied for the Password Authentication type").build());
                }
                if (!validationContext.getProperty(ACCUMULO_PASSWORD).isSet()){
                    problems.add(
                            new ValidationResult.Builder().valid(false).subject(ACCUMULO_PASSWORD.getName())
                                    .explanation("Password must be supplied for the Password Authentication type").build());
                }
                break;
            case KERBEROS:
                if (!validationContext.getProperty(KERBEROS_CREDENTIALS_SERVICE).isSet() && !validationContext.getProperty(KERBEROS_PASSWORD).isSet()){
                    problems.add(new ValidationResult.Builder().valid(false).subject(AUTHENTICATION_TYPE.getName())
                            .explanation("Either Kerberos Password or Kerberos Credential Service must be set").build());
                } else if (validationContext.getProperty(KERBEROS_CREDENTIALS_SERVICE).isSet() && validationContext.getProperty(KERBEROS_PASSWORD).isSet()){
                    problems.add(new ValidationResult.Builder().valid(false).subject(AUTHENTICATION_TYPE.getName())
                            .explanation("Kerberos Password and Kerberos Credential Service should not be filled out at the same time").build());
                } else if (validationContext.getProperty(KERBEROS_PASSWORD).isSet() && !validationContext.getProperty(KERBEROS_PRINCIPAL).isSet()) {
                    problems.add(new ValidationResult.Builder().valid(false).subject(KERBEROS_PRINCIPAL.getName())
                            .explanation("Kerberos Principal must be supplied when principal + password Kerberos authentication is used").build());
                } else if (validationContext.getProperty(KERBEROS_CREDENTIALS_SERVICE).isSet() && validationContext.getProperty(KERBEROS_PRINCIPAL).isSet()){
                    problems.add(new ValidationResult.Builder().valid(false).subject(KERBEROS_PRINCIPAL.getName())
                            .explanation("Kerberos Principal (for password) should not be filled out when principal + keytab Kerberos authentication is used").build());
                }
                break;
            default:
                problems.add(new ValidationResult.Builder().valid(false).subject(AUTHENTICATION_TYPE.getName()).explanation("Non supported Authentication type").build());
        }

        return problems;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, InterruptedException {
        if (!context.getProperty(INSTANCE_NAME).isSet() || !context.getProperty(ZOOKEEPER_QUORUM).isSet()) {
            throw new InitializationException("Instance name and Zookeeper Quorum must be specified");
        }

        final KerberosCredentialsService kerberosService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        final String instanceName = context.getProperty(INSTANCE_NAME).evaluateAttributeExpressions().getValue();
        final String zookeepers = context.getProperty(ZOOKEEPER_QUORUM).evaluateAttributeExpressions().getValue();
        this.authType = AuthenticationType.valueOf( context.getProperty(AUTHENTICATION_TYPE).getValue());

        final Properties clientConf = new Properties();
        clientConf.setProperty("instance.zookeepers", zookeepers);
        clientConf.setProperty("instance.name", instanceName);

        switch(authType){
            case PASSWORD:
                final String accumuloUser = context.getProperty(ACCUMULO_USER).evaluateAttributeExpressions().getValue();

                final AuthenticationToken token = new PasswordToken(context.getProperty(ACCUMULO_PASSWORD).getValue());

                this.client = Accumulo.newClient().from(clientConf).as(accumuloUser, token).build();
                break;
            case KERBEROS:
                final String principal;

                if (kerberosService == null) {
                    principal = context.getProperty(KERBEROS_PRINCIPAL).getValue();
                    this.kerberosUser = new KerberosPasswordUser(principal, context.getProperty(KERBEROS_PASSWORD).getValue());
                } else {
                    principal = kerberosService.getPrincipal();
                    this.kerberosUser = new KerberosKeytabUser(principal, kerberosService.getKeytab());
                }

                clientConf.setProperty("sasl.enabled", "true");
                clientConf.setProperty("sasl.qop", context.getProperty(ACCUMULO_SASL_QOP).getValue());

                //Client uses the currently logged in user's security context, so need to login first.
                Configuration conf = new Configuration();
                conf.set("hadoop.security.authentication", "kerberos");
                UserGroupInformation.setConfiguration(conf);
                final UserGroupInformation clientUgi = SecurityUtil.getUgiForKerberosUser(conf, kerberosUser);

                this.client = clientUgi.doAs((PrivilegedExceptionAction<AccumuloClient>) () ->
                        Accumulo.newClient().from(clientConf).as(principal, new KerberosToken()).build());
                break;
            default:
                throw new InitializationException("Not supported authentication type.");
        }
    }

    @Override
    public AccumuloClient getClient() {
        return client;
    }

    @Override
    public void renewTgtIfNecessary() {
        if (authType.equals(AuthenticationType.KERBEROS)) {
            SecurityUtil.checkTGTAndRelogin(getLogger(), kerberosUser);
        }
    }

    @OnDisabled
    public void shutdown() {
        if (client != null) {
            client.close();
        }
    }
}
