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
package org.apache.nifi.ssl;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;

@Tags({"ssl", "secure", "certificate", "keystore", "truststore", "jks", "p12", "pkcs12", "pkcs"})
@CapabilityDescription("Standard implementation of the SSLContextService. Provides the ability to configure "
        + "keystore and/or truststore properties once and reuse that configuration throughout the application")
public class StandardSSLContextService extends AbstractControllerService implements SSLContextService {

    public static final String STORE_TYPE_JKS = "JKS";
    public static final String STORE_TYPE_PKCS12 = "PKCS12";

    public static final PropertyDescriptor TRUSTSTORE = new PropertyDescriptor.Builder()
            .name("Truststore Filename")
            .description("The fully-qualified filename of the Truststore")
            .defaultValue(null)
            .addValidator(createFileExistsAndReadableValidator())
            .sensitive(false)
            .build();
    public static final PropertyDescriptor TRUSTSTORE_TYPE = new PropertyDescriptor.Builder()
            .name("Truststore Type")
            .description("The Type of the Truststore. Either JKS or PKCS12")
            .allowableValues(STORE_TYPE_JKS, STORE_TYPE_PKCS12)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(STORE_TYPE_JKS)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor TRUSTSTORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("Truststore Password")
            .description("The password for the Truststore")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor KEYSTORE = new PropertyDescriptor.Builder()
            .name("Keystore Filename")
            .description("The fully-qualified filename of the Keystore")
            .defaultValue(null)
            .addValidator(createFileExistsAndReadableValidator())
            .sensitive(false)
            .build();
    public static final PropertyDescriptor KEYSTORE_TYPE = new PropertyDescriptor.Builder()
            .name("Keystore Type")
            .description("The Type of the Keystore")
            .allowableValues(STORE_TYPE_JKS, STORE_TYPE_PKCS12)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(STORE_TYPE_JKS)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor KEYSTORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("Keystore Password")
            .defaultValue(null)
            .description("The password for the Keystore")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(KEYSTORE);
        props.add(KEYSTORE_PASSWORD);
        props.add(KEYSTORE_TYPE);
        props.add(TRUSTSTORE);
        props.add(TRUSTSTORE_PASSWORD);
        props.add(TRUSTSTORE_TYPE);
        properties = Collections.unmodifiableList(props);
    }
    private ConfigurationContext configContext;

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        configContext = context;

        final Collection<ValidationResult> results = new ArrayList<>();
        results.addAll(validateStore(context.getProperties(), KeystoreValidationGroup.KEYSTORE));
        results.addAll(validateStore(context.getProperties(), KeystoreValidationGroup.TRUSTSTORE));

        if (!results.isEmpty()) {
            final StringBuilder sb = new StringBuilder(this + " is not valid due to:");
            for (final ValidationResult result : results) {
                sb.append("\n").append(result.toString());
            }
            throw new InitializationException(sb.toString());
        }

        if (countNulls(context.getProperty(KEYSTORE).getValue(),
                context.getProperty(KEYSTORE_PASSWORD).getValue(),
                context.getProperty(KEYSTORE_TYPE).getValue(),
                context.getProperty(TRUSTSTORE).getValue(),
                context.getProperty(TRUSTSTORE_PASSWORD).getValue(),
                context.getProperty(TRUSTSTORE_TYPE).getValue()) >= 4) {
            throw new InitializationException(this + " does not have the KeyStore or the TrustStore populated");
        }

        // verify that the filename, password, and type match
        createSSLContext(ClientAuth.REQUIRED);
    }

    private static Validator createFileExistsAndReadableValidator() {
        return new Validator() {
            // Not using the FILE_EXISTS_VALIDATOR because the default is to
            // allow expression language
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                final String substituted;
                try {
                    substituted = context.newPropertyValue(input).evaluateAttributeExpressions().getValue();
                } catch (final Exception e) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation("Not a valid Expression Language value: " + e.getMessage())
                            .build();
                }

                final File file = new File(substituted);
                final boolean valid = file.exists() && file.canRead();
                final String explanation = valid ? null : "File " + file + " does not exist or cannot be read";
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(valid)
                        .explanation(explanation)
                        .build();
            }
        };
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();
        results.addAll(validateStore(validationContext.getProperties(), KeystoreValidationGroup.KEYSTORE));
        results.addAll(validateStore(validationContext.getProperties(), KeystoreValidationGroup.TRUSTSTORE));

        if (countNulls(validationContext.getProperty(KEYSTORE).getValue(),
                validationContext.getProperty(KEYSTORE_PASSWORD).getValue(),
                validationContext.getProperty(KEYSTORE_TYPE).getValue(),
                validationContext.getProperty(TRUSTSTORE).getValue(),
                validationContext.getProperty(TRUSTSTORE_PASSWORD).getValue(),
                validationContext.getProperty(TRUSTSTORE_TYPE).getValue())
                >= 4) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName() + " : " + getIdentifier())
                    .valid(false)
                    .explanation("Does not have the KeyStore or the TrustStore populated")
                    .build());
        }
        if (results.isEmpty()) {
            // verify that the filename, password, and type match
            try {
                createSSLContext(ClientAuth.REQUIRED);
            } catch (ProcessException e) {
                results.add(new ValidationResult.Builder()
                        .subject(getClass().getSimpleName() + " : " + getIdentifier())
                        .valid(false)
                        .explanation(e.getMessage())
                        .build());
            }
        }
        return results;
    }

    @Override
    public SSLContext createSSLContext(final ClientAuth clientAuth) throws ProcessException {
        try {
            final String keystoreFile = configContext.getProperty(KEYSTORE).getValue();
            if (keystoreFile == null) {
                return SslContextFactory.createTrustSslContext(
                        configContext.getProperty(TRUSTSTORE).getValue(),
                        configContext.getProperty(TRUSTSTORE_PASSWORD).getValue().toCharArray(),
                        configContext.getProperty(TRUSTSTORE_TYPE).getValue());
            }
            final String truststoreFile = configContext.getProperty(TRUSTSTORE).getValue();
            if (truststoreFile == null) {
                return SslContextFactory.createSslContext(
                        configContext.getProperty(KEYSTORE).getValue(),
                        configContext.getProperty(KEYSTORE_PASSWORD).getValue().toCharArray(),
                        configContext.getProperty(KEYSTORE_TYPE).getValue());
            }

            return SslContextFactory.createSslContext(
                    configContext.getProperty(KEYSTORE).getValue(),
                    configContext.getProperty(KEYSTORE_PASSWORD).getValue().toCharArray(),
                    configContext.getProperty(KEYSTORE_TYPE).getValue(),
                    configContext.getProperty(TRUSTSTORE).getValue(),
                    configContext.getProperty(TRUSTSTORE_PASSWORD).getValue().toCharArray(),
                    configContext.getProperty(TRUSTSTORE_TYPE).getValue(),
                    org.apache.nifi.security.util.SslContextFactory.ClientAuth.valueOf(clientAuth.name()));
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public String getTrustStoreFile() {
        return configContext.getProperty(TRUSTSTORE).getValue();
    }

    @Override
    public String getTrustStoreType() {
        return configContext.getProperty(TRUSTSTORE_TYPE).getValue();
    }

    @Override
    public String getTrustStorePassword() {
        return configContext.getProperty(TRUSTSTORE_PASSWORD).getValue();
    }

    @Override
    public boolean isTrustStoreConfigured() {
        return getTrustStoreFile() != null && getTrustStorePassword() != null && getTrustStoreType() != null;
    }

    @Override
    public String getKeyStoreFile() {
        return configContext.getProperty(KEYSTORE).getValue();
    }

    @Override
    public String getKeyStoreType() {
        return configContext.getProperty(KEYSTORE_TYPE).getValue();
    }

    @Override
    public String getKeyStorePassword() {
        return configContext.getProperty(KEYSTORE_PASSWORD).getValue();
    }

    @Override
    public boolean isKeyStoreConfigured() {
        return getKeyStoreFile() != null && getKeyStorePassword() != null && getKeyStoreType() != null;
    }

    private static Collection<ValidationResult> validateStore(final Map<PropertyDescriptor, String> properties,
            final KeystoreValidationGroup keyStoreOrTrustStore) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String filename;
        final String password;
        final String type;

        if (keyStoreOrTrustStore == KeystoreValidationGroup.KEYSTORE) {
            filename = properties.get(KEYSTORE);
            password = properties.get(KEYSTORE_PASSWORD);
            type = properties.get(KEYSTORE_TYPE);
        } else {
            filename = properties.get(TRUSTSTORE);
            password = properties.get(TRUSTSTORE_PASSWORD);
            type = properties.get(TRUSTSTORE_TYPE);
        }

        final String keystoreDesc = (keyStoreOrTrustStore == KeystoreValidationGroup.KEYSTORE) ? "Keystore" : "Truststore";

        final int nulls = countNulls(filename, password, type);
        if (nulls != 3 && nulls != 0) {
            results.add(new ValidationResult.Builder().valid(false).explanation("Must set either 0 or 3 properties for " + keystoreDesc)
                    .subject(keystoreDesc + " Properties").build());
        } else if (nulls == 0) {
            // all properties were filled in.
            final File file = new File(filename);
            if (!file.exists() || !file.canRead()) {
                results.add(new ValidationResult.Builder()
                        .valid(false)
                        .subject(keystoreDesc + " Properties")
                        .explanation("Cannot access file " + file.getAbsolutePath())
                        .build());
            } else {
                try {
                    final boolean storeValid = CertificateUtils
                            .isStoreValid(file.toURI().toURL(), KeystoreType.valueOf(type), password.toCharArray());
                    if (!storeValid) {
                        results.add(new ValidationResult.Builder()
                                .subject(keystoreDesc + " Properties")
                                .valid(false)
                                .explanation("Invalid KeyStore Password or Type specified for file " + filename)
                                .build());
                    }
                } catch (MalformedURLException e) {
                    results.add(new ValidationResult.Builder()
                            .subject(keystoreDesc + " Properties")
                            .valid(false)
                            .explanation("Malformed URL from file: " + e)
                            .build());
                }
            }
        }

        return results;
    }

    private static int countNulls(Object... objects) {
        int count = 0;
        for (final Object x : objects) {
            if (x == null) {
                count++;
            }
        }

        return count;
    }

    public static enum KeystoreValidationGroup {

        KEYSTORE, TRUSTSTORE
    }

    @Override
    public String toString() {
        return "SSLContextService[id=" + getIdentifier() + "]";
    }
}
