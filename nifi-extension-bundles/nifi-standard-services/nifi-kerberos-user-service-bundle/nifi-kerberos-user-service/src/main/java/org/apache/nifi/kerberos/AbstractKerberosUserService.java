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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Base class for KerberosUserService implementations.
 */
public abstract class AbstractKerberosUserService extends AbstractControllerService implements KerberosUserService {

    static final PropertyDescriptor PRINCIPAL = new PropertyDescriptor.Builder()
            .name("Kerberos Principal")
            .description("Kerberos principal to authenticate as. Requires nifi.kerberos.krb5.file to be set in your nifi.properties")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    private File kerberosConfigFile;
    private volatile String principal;

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
        properties.add(PRINCIPAL);
        properties.addAll(getAdditionalProperties());
        return properties;
    }

    @OnEnabled
    public void setConfiguredValues(final ConfigurationContext context) {
        this.principal = context.getProperty(PRINCIPAL).evaluateAttributeExpressions().getValue();
        setAdditionalConfiguredValues(context);
    }

    /**
     * @return the configured principal
     */
    protected String getPrincipal() {
        return principal;
    }

    /**
     * @return a non-null list additional properties to add
     */
    protected abstract List<PropertyDescriptor> getAdditionalProperties();

    /**
     * Allow sub-classes to obtain additional configuration values during @OnEnabled.
     *
     * @param context the config context
     */
    protected abstract void setAdditionalConfiguredValues(final ConfigurationContext context);


}
