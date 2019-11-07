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
package org.apache.nifi.kerberos;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.krb.KerberosPrincipalParser;

@CapabilityDescription("Provides a mechanism for specifying a Keytab and a Principal that other components are able to use in order to "
        + "perform authentication using Kerberos. By encapsulating this information into a Controller Service and allowing other components to make use of it "
        + "(as opposed to specifying the principal and keytab directly in the processor) an administrator is able to choose which users are allowed to "
        + "use which keytabs and principals. This provides a more robust security model for multi-tenant use cases.")
@Tags({"Kerberos", "Keytab", "Principal", "Credentials", "Authentication", "Security"})
@Restricted(restrictions = {
        @Restriction(requiredPermission = RequiredPermission.ACCESS_KEYTAB, explanation = "Allows user to define a Keytab and principal that can then be used by other components.")
})
public class KeytabCredentialsService extends AbstractControllerService implements KerberosCredentialsService {

    private static final String PRINCIPAL_QUALIFIER_STRATEGY_VALUE_NONE = "principal-instance-qualifier-strategy-none";
    private static final String PRINCIPAL_QUALIFIER_STRATEGY_VALUE_HOSTNAME = "principal-instance-qualifier-strategy-hostname";
    private static final String PRINCIPAL_QUALIFIER_STRATEGY_VALUE_FQDN = "principal-instance-qualifier-strategy-fqdn";

    static final PropertyDescriptor PRINCIPAL = new PropertyDescriptor.Builder()
            .name("Kerberos Principal")
            .description("Kerberos principal to authenticate as. Requires nifi.kerberos.krb5.file to be set in your nifi.properties")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final PropertyDescriptor KEYTAB = new PropertyDescriptor.Builder()
            .name("Kerberos Keytab")
            .description("Kerberos keytab associated with the principal. Requires nifi.kerberos.krb5.file to be set in your nifi.properties")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final AllowableValue PRINCIPAL_QUALIFIER_STRATEGY_NONE = new AllowableValue(PRINCIPAL_QUALIFIER_STRATEGY_VALUE_NONE,
            "No Principal Instance Qualification",
            "No modifications will be made to the principal defined in the " + PRINCIPAL.getDisplayName() + " property");
    static final AllowableValue PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME = new AllowableValue(PRINCIPAL_QUALIFIER_STRATEGY_VALUE_HOSTNAME,
            "Hostname Principal Instance Qualification",
            "Qualifies the instance on the principal defined in the " + PRINCIPAL.getDisplayName() + " property with the processing node's hostname " +
                    "if an instance is not explicitly designated");
    static final AllowableValue PRINCIPAL_QUALIFIER_STRATEGY_FQDN = new AllowableValue(PRINCIPAL_QUALIFIER_STRATEGY_VALUE_FQDN,
            "FQDN Principal Instance Qualification",
            "Qualifies the instance on the principal defined in the " + PRINCIPAL.getDisplayName() + " property with the processing node's fully-qualified domain name " +
                    "if an instance is not explicitly designated");

    static final PropertyDescriptor PRINCIPAL_INSTANCE_QUALIFIER_STRATEGY = new PropertyDescriptor.Builder()
            .name("Kerberos Principal Instance Qualifier Strategy")
            .description("Qualifies the principal defined in the " + PRINCIPAL.getDisplayName() + " property with an instance based on the selected strategy")
            .allowableValues(PRINCIPAL_QUALIFIER_STRATEGY_NONE, PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME, PRINCIPAL_QUALIFIER_STRATEGY_FQDN)
            .defaultValue(PRINCIPAL_QUALIFIER_STRATEGY_NONE.getValue())
            .required(true)
            .build();

    /**
     * Property used to determine the hostname of the node on which this service is running via expression language.
     * This is used when qualifying the instance of a given kerberos principal using the
     * {@link KeytabCredentialsService#PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME} strategy.
     */
    static final PropertyDescriptor NODE_HOSTNAME_EXPRESSION = new PropertyDescriptor.Builder()
            .name("node-hostname")
            .displayName("Node Hostname Expression")
            .description("The expression used to determine the hostname when the " + PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME.getDisplayName() + " strategy is used")
            .required(true)
            .allowableValues(new AllowableValue("${hostname()}"))
            .defaultValue("${hostname()}")
            .build();

    /**
     * Property used to determine the fully-qualified domain name of the node on which this service is running via expression language.
     * This is used when qualifying the instance of a given kerberos principal using the
     * {@link KeytabCredentialsService#PRINCIPAL_QUALIFIER_STRATEGY_FQDN} strategy.
     */
    static final PropertyDescriptor NODE_FQDN_EXPRESSION = new PropertyDescriptor.Builder()
            .name("node-fqdn")
            .displayName("Node FQDN Expression")
            .required(true)
            .allowableValues(new AllowableValue("${hostname(true)}"))
            .defaultValue("${hostname(true)}")
            .build();

    private File kerberosConfigFile;
    private volatile String principal;
    private volatile String keytab;
    private volatile String principalInstanceQualifierStrategy;

    @Override
    protected final void init(final ControllerServiceInitializationContext config) {
        kerberosConfigFile = config.getKerberosConfigurationFile();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        // Check that the Kerberos configuration is set
        if (kerberosConfigFile == null) {
            results.add(new ValidationResult.Builder()
                    .subject("Kerberos Configuration File")
                    .valid(false)
                    .explanation("The nifi.kerberos.krb5.file property must be set in nifi.properties in order to use Kerberos authentication")
                    .build());
        } else if (!kerberosConfigFile.canRead()) {
            // Check that the Kerberos configuration is readable
            results.add(new ValidationResult.Builder()
                    .subject("Kerberos Configuration File")
                    .valid(false)
                    .explanation("Unable to read configured Kerberos Configuration File " + kerberosConfigFile.getAbsolutePath() + ", which is specified in nifi.properties. "
                            + "Please ensure that the path is valid and that NiFi has adequate permissions to read the file.")
                    .build());
        }

        return results;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(2);
        properties.add(KEYTAB);
        properties.add(PRINCIPAL);
        properties.add(PRINCIPAL_INSTANCE_QUALIFIER_STRATEGY);
        properties.add(NODE_HOSTNAME_EXPRESSION);
        properties.add(NODE_FQDN_EXPRESSION);
        return properties;
    }

    @OnEnabled
    public void setConfiguredValues(final ConfigurationContext context) {
        this.keytab = context.getProperty(KEYTAB).evaluateAttributeExpressions().getValue();
        this.principal = context.getProperty(PRINCIPAL).evaluateAttributeExpressions().getValue();
        this.principalInstanceQualifierStrategy = context.getProperty(PRINCIPAL_INSTANCE_QUALIFIER_STRATEGY).getValue();
    }

    @Override
    public String getKeytab() {
        return keytab;
    }

    @Override
    public String getPrincipal() {
        final String strategyQualifiedPrincipal;
        final String qualifiedInstance;

        String realm = KerberosPrincipalParser.getRealm(principal);
        String shortname = KerberosPrincipalParser.getShortname(principal);
        String instance = KerberosPrincipalParser.getInstance(principal);

        switch (principalInstanceQualifierStrategy) {
            case PRINCIPAL_QUALIFIER_STRATEGY_VALUE_HOSTNAME:
                qualifiedInstance = getConfigurationContext().getProperty(NODE_HOSTNAME_EXPRESSION).evaluateAttributeExpressions().getValue();
                break;
            case PRINCIPAL_QUALIFIER_STRATEGY_VALUE_FQDN:
                qualifiedInstance = getConfigurationContext().getProperty(NODE_FQDN_EXPRESSION).evaluateAttributeExpressions().getValue();
                break;
            case PRINCIPAL_QUALIFIER_STRATEGY_VALUE_NONE:
                // do nothing, let control flow through to the default case
            default:
                qualifiedInstance = null;
        }

        strategyQualifiedPrincipal = shortname + (instance != null ? '/' + instance : (qualifiedInstance != null ? '/' + qualifiedInstance : "")) + (realm != null ? '@' + realm : "");
        return strategyQualifiedPrincipal;
    }
}
