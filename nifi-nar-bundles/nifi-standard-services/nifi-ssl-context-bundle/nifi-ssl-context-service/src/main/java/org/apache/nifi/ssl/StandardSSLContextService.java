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
import org.apache.nifi.components.PropertyValue;
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

@Tags({"ssl", "secure", "certificate", "keystore", "truststore", "jks", "p12", "pkcs12", "pkcs", "tls"})
@CapabilityDescription("Standard implementation of the SSLContextService. Provides the ability to configure "
        + "keystore and/or truststore properties once and reuse that configuration throughout the application. "
        + "This service can be used to communicate with both legacy and modern systems. If you only need to "
        + "communicate with non-legacy systems, then the StandardRestrictedSSLContextService is recommended as it only "
        + "allows a specific set of SSL protocols to be chosen.")
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
            .sensitive(false)
            .build();
    public static final PropertyDescriptor KEYSTORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("Keystore Password")
            .defaultValue(null)
            .description("The password for the Keystore")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    static final PropertyDescriptor KEY_PASSWORD = new PropertyDescriptor.Builder()
            .name("key-password")
            .displayName("Key Password")
            .description("The password for the key. If this is not specified, but the Keystore Filename, Password, and Type are specified, "
                    + "then the Keystore Password will be assumed to be the same as the Key Password.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .required(false)
            .build();
    public static final PropertyDescriptor SSL_ALGORITHM = new PropertyDescriptor.Builder()
            .name("SSL Protocol")
            .displayName("TLS Protocol")
            .defaultValue("TLS")
            .required(false)
            .allowableValues(SSLContextService.buildAlgorithmAllowableValues())
            .description("The algorithm to use for this TLS/SSL context")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    private static final List<PropertyDescriptor> properties;
    protected ConfigurationContext configContext;
    private boolean isValidated;

    // TODO: This can be made configurable if necessary
    private static final int VALIDATION_CACHE_EXPIRATION = 5;
    private int validationCacheCount = 0;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(KEYSTORE);
        props.add(KEYSTORE_PASSWORD);
        props.add(KEY_PASSWORD);
        props.add(KEYSTORE_TYPE);
        props.add(TRUSTSTORE);
        props.add(TRUSTSTORE_PASSWORD);
        props.add(TRUSTSTORE_TYPE);
        props.add(SSL_ALGORITHM);
        properties = Collections.unmodifiableList(props);
    }

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

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        resetValidationCache();
    }

    private static Validator createFileExistsAndReadableValidator() {
        return new Validator() {
            // Not using the FILE_EXISTS_VALIDATOR because the default is to
            // allow expression language
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                final File file = new File(input);
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

        if (isValidated) {
            validationCacheCount++;
            if (validationCacheCount > VALIDATION_CACHE_EXPIRATION) {
                resetValidationCache();
            } else {
                return results;
            }
        }

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
                verifySslConfig(validationContext);
            } catch (ProcessException e) {
                results.add(new ValidationResult.Builder()
                        .subject(getClass().getSimpleName() + " : " + getIdentifier())
                        .valid(false)
                        .explanation(e.getMessage())
                        .build());
            }
        }

        isValidated = results.isEmpty();

        return results;
    }

    private void resetValidationCache() {
        validationCacheCount = 0;
        isValidated = false;
    }

    protected int getValidationCacheExpiration() {
        return VALIDATION_CACHE_EXPIRATION;
    }

    protected String getSSLProtocolForValidation(final ValidationContext validationContext) {
        return validationContext.getProperty(SSL_ALGORITHM).getValue();
    }

    private void verifySslConfig(final ValidationContext validationContext) throws ProcessException {
        final String protocol = getSSLProtocolForValidation(validationContext);
        try {
            final PropertyValue keyPasswdProp = validationContext.getProperty(KEY_PASSWORD);
            final char[] keyPassword = keyPasswdProp.isSet() ? keyPasswdProp.getValue().toCharArray() : null;

            final String keystoreFile = validationContext.getProperty(KEYSTORE).getValue();
            if (keystoreFile == null) {
                SslContextFactory.createTrustSslContext(
                        validationContext.getProperty(TRUSTSTORE).getValue(),
                        validationContext.getProperty(TRUSTSTORE_PASSWORD).getValue().toCharArray(),
                        validationContext.getProperty(TRUSTSTORE_TYPE).getValue(),
                        protocol);
                return;
            }
            final String truststoreFile = validationContext.getProperty(TRUSTSTORE).getValue();
            if (truststoreFile == null) {
                SslContextFactory.createSslContext(
                        validationContext.getProperty(KEYSTORE).getValue(),
                        validationContext.getProperty(KEYSTORE_PASSWORD).getValue().toCharArray(),
                        keyPassword,
                        validationContext.getProperty(KEYSTORE_TYPE).getValue(),
                        protocol);
                return;
            }

            SslContextFactory.createSslContext(
                    validationContext.getProperty(KEYSTORE).getValue(),
                    validationContext.getProperty(KEYSTORE_PASSWORD).getValue().toCharArray(),
                    keyPassword,
                    validationContext.getProperty(KEYSTORE_TYPE).getValue(),
                    validationContext.getProperty(TRUSTSTORE).getValue(),
                    validationContext.getProperty(TRUSTSTORE_PASSWORD).getValue().toCharArray(),
                    validationContext.getProperty(TRUSTSTORE_TYPE).getValue(),
                    org.apache.nifi.security.util.SslContextFactory.ClientAuth.REQUIRED,
                    protocol);
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public SSLContext createSSLContext(final ClientAuth clientAuth) throws ProcessException {
        final String protocol = getSslAlgorithm();
        try {
            final PropertyValue keyPasswdProp = configContext.getProperty(KEY_PASSWORD);
            final char[] keyPassword = keyPasswdProp.isSet() ? keyPasswdProp.getValue().toCharArray() : null;

            final String keystoreFile = configContext.getProperty(KEYSTORE).getValue();
            if (keystoreFile == null) {
                // If keystore not specified, create SSL Context based only on trust store.
                return SslContextFactory.createTrustSslContext(
                        configContext.getProperty(TRUSTSTORE).getValue(),
                        configContext.getProperty(TRUSTSTORE_PASSWORD).getValue().toCharArray(),
                        configContext.getProperty(TRUSTSTORE_TYPE).getValue(),
                        protocol);
            }

            final String truststoreFile = configContext.getProperty(TRUSTSTORE).getValue();
            if (truststoreFile == null) {
                // If truststore not specified, create SSL Context based only on key store.
                return SslContextFactory.createSslContext(
                        configContext.getProperty(KEYSTORE).getValue(),
                        configContext.getProperty(KEYSTORE_PASSWORD).getValue().toCharArray(),
                        keyPassword,
                        configContext.getProperty(KEYSTORE_TYPE).getValue(),
                        protocol);
            }

            return SslContextFactory.createSslContext(
                    configContext.getProperty(KEYSTORE).getValue(),
                    configContext.getProperty(KEYSTORE_PASSWORD).getValue().toCharArray(),
                    keyPassword,
                    configContext.getProperty(KEYSTORE_TYPE).getValue(),
                    configContext.getProperty(TRUSTSTORE).getValue(),
                    configContext.getProperty(TRUSTSTORE_PASSWORD).getValue().toCharArray(),
                    configContext.getProperty(TRUSTSTORE_TYPE).getValue(),
                    org.apache.nifi.security.util.SslContextFactory.ClientAuth.valueOf(clientAuth.name()),
                    protocol);
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
    public String getKeyPassword() {
        return configContext.getProperty(KEY_PASSWORD).getValue();
    }

    @Override
    public boolean isKeyStoreConfigured() {
        return getKeyStoreFile() != null && getKeyStorePassword() != null && getKeyStoreType() != null;
    }

    @Override
    public String getSslAlgorithm() {
        return configContext.getProperty(SSL_ALGORITHM).getValue();
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
                    final boolean storeValid = CertificateUtils.isStoreValid(file.toURI().toURL(), KeystoreType.valueOf(type), password.toCharArray());
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

    public enum KeystoreValidationGroup {

        KEYSTORE, TRUSTSTORE
    }

    @Override
    public String toString() {
        return "SSLContextService[id=" + getIdentifier() + "]";
    }
}
