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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@CapabilityDescription("Provides a mechanism for specifying a Keytab and a Principal that other components are able to use in order to "
    + "perform authentication using Kerberos. By encapsulating this information into a Controller Service and allowing other components to make use of it "
    + "(as opposed to specifying the principal and keytab directly in the processor) an administrator is able to choose which users are allowed to "
    + "use which keytabs and principals. This provides a more robust security model for multi-tenant use cases.")
@Tags({"Kerberos", "Keytab", "Principal", "Credentials", "Authentication", "Security"})
@Restricted(restrictions = {
    @Restriction(requiredPermission = RequiredPermission.ACCESS_KEYTAB, explanation = "Allows user to define a Keytab and principal that can then be used by other components.")
})
public class KeytabCredentialsService extends AbstractControllerService implements KerberosCredentialsService {

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

    private File kerberosConfigFile;
    private volatile String principal;
    private volatile String keytab;

    @Override
    protected final void init(final ControllerServiceInitializationContext config) throws InitializationException {
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
        return properties;
    }

    @OnEnabled
    public void setConfiguredValues(final ConfigurationContext context) {
        this.keytab = context.getProperty(KEYTAB).evaluateAttributeExpressions().getValue();
        this.principal = context.getProperty(PRINCIPAL).evaluateAttributeExpressions().getValue();
    }

    @Override
    public String getKeytab() {
        return keytab;
    }

    @Override
    public String getPrincipal() {
        return principal;
    }
}
