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
package org.apache.nifi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * All processors and controller services that need properties for Kerberos
 * Principal and Keytab should obtain them through this class by calling:
 *
 * KerberosProperties props =
 * KerberosProperties.create(NiFiProperties.getInstance())
 *
 * The properties can be accessed from the resulting KerberosProperties
 * instance.
 */
public class KerberosProperties {

    private final File kerberosConfigFile;
    private final Validator kerberosConfigValidator;
    private final PropertyDescriptor kerberosPrincipal;
    private final PropertyDescriptor kerberosKeytab;

    /**
     * Instantiate a KerberosProperties object but keep in mind it is
     * effectively a singleton because the krb5.conf file needs to be set as a
     * system property which this constructor will take care of.
     *
     * @param kerberosConfigFile file of krb5.conf
     */
    public KerberosProperties(final File kerberosConfigFile) {
        this.kerberosConfigFile = kerberosConfigFile;

        this.kerberosConfigValidator = new Validator() {
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                // Check that the Kerberos configuration is set
                if (kerberosConfigFile == null) {
                    return new ValidationResult.Builder()
                            .subject(subject).input(input).valid(false)
                            .explanation("you are missing the nifi.kerberos.krb5.file property which "
                                    + "must be set in order to use Kerberos")
                            .build();
                }

                // Check that the Kerberos configuration is readable
                if (!kerberosConfigFile.canRead()) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                            .explanation(String.format("unable to read Kerberos config [%s], please make sure the path is valid "
                                    + "and nifi has adequate permissions", kerberosConfigFile.getAbsoluteFile()))
                            .build();
                }

                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }
        };

        this.kerberosPrincipal = new PropertyDescriptor.Builder()
                .name("Kerberos Principal")
                .required(false)
                .description("Kerberos principal to authenticate as. Requires nifi.kerberos.krb5.file to be set in your nifi.properties")
                .addValidator(kerberosConfigValidator)
                .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
                .expressionLanguageSupported(true)
                .build();

        this.kerberosKeytab = new PropertyDescriptor.Builder()
                .name("Kerberos Keytab").required(false)
                .description("Kerberos keytab associated with the principal. Requires nifi.kerberos.krb5.file to be set in your nifi.properties")
                .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                .addValidator(kerberosConfigValidator)
                .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
                .expressionLanguageSupported(true)
                .build();
    }

    public File getKerberosConfigFile() {
        return kerberosConfigFile;
    }

    public Validator getKerberosConfigValidator() {
        return kerberosConfigValidator;
    }

    public PropertyDescriptor getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    public PropertyDescriptor getKerberosKeytab() {
        return kerberosKeytab;
    }

    public static List<ValidationResult> validatePrincipalAndKeytab(final String subject, final Configuration config, final String principal, final String keytab, final ComponentLog logger) {
        final List<ValidationResult> results = new ArrayList<>();

        // if security is enabled then the keytab and principal are required
        final boolean isSecurityEnabled = SecurityUtil.isSecurityEnabled(config);

        final boolean blankPrincipal = (principal == null || principal.isEmpty());
        if (isSecurityEnabled && blankPrincipal) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(subject)
                    .explanation("Kerberos Principal must be provided when using a secure configuration")
                    .build());
        }

        final boolean blankKeytab = (keytab == null || keytab.isEmpty());
        if (isSecurityEnabled && blankKeytab) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(subject)
                    .explanation("Kerberos Keytab must be provided when using a secure configuration")
                    .build());
        }

        if (!isSecurityEnabled && (!blankPrincipal || !blankKeytab)) {
            logger.warn("Configuration does not have security enabled, Keytab and Principal will be ignored");
        }

        return results;
    }

}
