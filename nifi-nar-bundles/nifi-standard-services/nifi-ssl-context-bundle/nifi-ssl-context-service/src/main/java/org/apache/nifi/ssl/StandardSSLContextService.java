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
import javax.net.ssl.TrustManager;

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
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.StringUtils;

@Tags({"ssl", "secure", "certificate", "keystore", "truststore", "jks", "p12", "pkcs12", "pkcs", "tls"})
@CapabilityDescription("Standard implementation of the SSLContextService. Provides the ability to configure "
        + "keystore and/or truststore properties once and reuse that configuration throughout the application. "
        + "This service can be used to communicate with both legacy and modern systems. If you only need to "
        + "communicate with non-legacy systems, then the StandardRestrictedSSLContextService is recommended as it only "
        + "allows a specific set of SSL protocols to be chosen.")
public class StandardSSLContextService extends AbstractControllerService implements SSLContextService {

    public static final String STORE_TYPE_JKS = "JKS";
    public static final String STORE_TYPE_PKCS12 = "PKCS12";

    // Shared description for other SSL context services
    public static final String COMMON_TLS_PROTOCOL_DESCRIPTION = "The algorithm to use for this TLS/SSL context. \"TLS\" will instruct NiFi to allow all supported protocol versions " +
            "and choose the highest available protocol for each connection. " +
            "Java 8 enabled TLSv1.2, which is now the lowest version supported for incoming connections. " +
            "Java 11 enabled TLSv1.3. Depending on the version of Java NiFi is running on, different protocol versions will be available. " +
            "With \"TLS\" selected, as new protocol versions are made available, NiFi will automatically select them. " +
            "It is recommended unless a specific protocol version is needed. ";

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
            .addValidator(Validator.VALID)
            .required(false)
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
            .description(COMMON_TLS_PROTOCOL_DESCRIPTION +
                    "For outgoing connections, legacy protocol versions like \"TLSv1.0\" are supported, but discouraged unless necessary. ")
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
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        resetValidationCache();
    }

    private static Validator createFileExistsAndReadableValidator() {
        // Not using the FILE_EXISTS_VALIDATOR because the default is to allow expression language
        return (subject, input, context) -> {
            final File file = new File(input);
            final boolean valid = file.exists() && file.canRead();
            final String explanation = valid ? null : "File " + file + " does not exist or cannot be read";
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(valid)
                    .explanation(explanation)
                    .build();
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

    /**
     * Returns a {@link TlsConfiguration} configured with the current properties of the controller
     * service. This is useful for transferring the TLS configuration values between services.
     *
     * @return the populated TlsConfiguration
     */
    @Override
    public TlsConfiguration createTlsConfiguration() {
        return new StandardTlsConfiguration(getKeyStoreFile(), getKeyStorePassword(),
                getKeyPassword(), getKeyStoreType(), getTrustStoreFile(),
                getTrustStorePassword(), getTrustStoreType(), getSslAlgorithm());
    }

    /**
     * Returns a configured {@link SSLContext} from the populated configuration values. This method is preferred
     * over the overloaded method which accepts the deprecated {@link ClientAuth} enum.
     *
     * @param clientAuth the desired level of client authentication
     * @return the configured SSLContext
     * @throws ProcessException if there is a problem configuring the context
     */
    @Override
    public SSLContext createSSLContext(final org.apache.nifi.security.util.ClientAuth clientAuth) throws ProcessException {
        try {
            final TlsConfiguration tlsConfiguration = createTlsConfiguration();
            if (!tlsConfiguration.isTruststorePopulated()) {
                getLogger().warn("Trust Store properties not found: using platform default Certificate Authorities");
            }
            final TrustManager[] trustManagers = SslContextFactory.getTrustManagers(tlsConfiguration);
            return SslContextFactory.createSslContext(tlsConfiguration, trustManagers, clientAuth);
        } catch (TlsException e) {
            getLogger().error("Encountered an error creating the SSL context from the SSL context service: {}", new String[]{e.getLocalizedMessage()});
            throw new ProcessException("Error creating SSL context", e);
        }
    }

    /**
     * Returns a configured {@link SSLContext} from the populated configuration values. This method is deprecated
     * due to the use of the deprecated {@link ClientAuth} enum and the overloaded method
     * ({@link #createSSLContext(org.apache.nifi.security.util.ClientAuth)}) is preferred.
     *
     * @param clientAuth the desired level of client authentication
     * @return the configured SSLContext
     * @throws ProcessException if there is a problem configuring the context
     */
    @Override
    public SSLContext createSSLContext(final ClientAuth clientAuth) throws ProcessException {
        org.apache.nifi.security.util.ClientAuth resolvedClientAuth = org.apache.nifi.security.util.ClientAuth.valueOf(clientAuth.name());
            return createSSLContext(resolvedClientAuth);
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
        PropertyValue truststorePassword = configContext.getProperty(TRUSTSTORE_PASSWORD);
        return truststorePassword.isSet() ? truststorePassword.getValue() : "";
    }

    @Override
    public boolean isTrustStoreConfigured() {
        return getTrustStoreFile() != null && getTrustStoreType() != null;
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

    /**
     * Returns a list of {@link ValidationResult}s for the provided
     * keystore/truststore properties. Called during
     * {@link #customValidate(ValidationContext)}.
     *
     * @param properties           the map of component properties
     * @param keyStoreOrTrustStore an enum {@link KeystoreValidationGroup} indicating keystore or truststore because logic is different
     * @return the list of validation results (empty means valid)
     */
    private static Collection<ValidationResult> validateStore(final Map<PropertyDescriptor, String> properties,
                                                              final KeystoreValidationGroup keyStoreOrTrustStore) {
        List<ValidationResult> results;

        if (keyStoreOrTrustStore == KeystoreValidationGroup.KEYSTORE) {
            results = validateKeystore(properties);
        } else {
            results = validateTruststore(properties);
        }

        if (keystorePropertiesEmpty(properties) && truststorePropertiesEmpty(properties)) {
            results.add(new ValidationResult.Builder().valid(false).explanation("Either the keystore and/or truststore must be populated").subject("Keystore/truststore properties").build());
        }

        return results;
    }

    private static boolean keystorePropertiesEmpty(Map<PropertyDescriptor, String> properties) {
        return StringUtils.isBlank(properties.get(KEYSTORE)) && StringUtils.isBlank(properties.get(KEYSTORE_PASSWORD)) && StringUtils.isBlank(properties.get(KEYSTORE_TYPE));
    }

    private static boolean truststorePropertiesEmpty(Map<PropertyDescriptor, String> properties) {
        return StringUtils.isBlank(properties.get(TRUSTSTORE)) && StringUtils.isBlank(properties.get(TRUSTSTORE_PASSWORD)) && StringUtils.isBlank(properties.get(TRUSTSTORE_TYPE));
    }

    /**
     * Returns the count of {@code null} objects in the parameters. Used for keystore/truststore validation.
     *
     * @param objects a variable array of objects, some of which can be null
     * @return the count of provided objects which were null
     */
    private static int countNulls(Object... objects) {
        int count = 0;
        for (final Object x : objects) {
            if (x == null) {
                count++;
            }
        }

        return count;
    }

    /**
     * Returns a list of {@link ValidationResult}s for keystore validity checking. Ensures none or all of the properties
     * are populated; if populated, validates the keystore file on disk and password as well.
     *
     * @param properties the component properties
     * @return the list of validation results (empty is valid)
     */
    private static List<ValidationResult> validateKeystore(final Map<PropertyDescriptor, String> properties) {
        final List<ValidationResult> results = new ArrayList<>();

        final String filename = properties.get(KEYSTORE);
        final String password = properties.get(KEYSTORE_PASSWORD);
        final String keyPassword = properties.get(KEY_PASSWORD);
        final String type = properties.get(KEYSTORE_TYPE);

        final int nulls = countNulls(filename, password, type);
        if (nulls != 3 && nulls != 0) {
            results.add(new ValidationResult.Builder().valid(false).explanation("Must set either 0 or 3 properties for Keystore")
                    .subject("Keystore Properties").build());
        } else if (nulls == 0) {
            // all properties were filled in.
            List<ValidationResult> fileValidationResults = validateKeystoreFile(filename, password, keyPassword, type);
            results.addAll(fileValidationResults);
        }

        // If nulls == 3, no values were populated, so just return

        return results;
    }


    /**
     * Returns a list of {@link ValidationResult}s for truststore validity checking. Ensures none of the properties
     * are populated or at least filename and type are populated; if populated, validates the truststore file on disk
     * and password as well.
     *
     * @param properties the component properties
     * @return the list of validation results (empty is valid)
     */
    private static List<ValidationResult> validateTruststore(final Map<PropertyDescriptor, String> properties) {
        String filename = properties.get(TRUSTSTORE);
        String password = properties.get(TRUSTSTORE_PASSWORD);
        String type = properties.get(TRUSTSTORE_TYPE);

        List<ValidationResult> results = new ArrayList<>();

        if (!StringUtils.isBlank(filename) && !StringUtils.isBlank(type)) {
            // In this case both the filename and type are populated, which is sufficient
            results.addAll(validateTruststoreFile(filename, password, type));
        } else {
            // The filename or type are blank; all values must be unpopulated for this to be valid
            if (!StringUtils.isBlank(filename) || !StringUtils.isBlank(type)) {
                results.add(new ValidationResult.Builder().valid(false).explanation("If the truststore filename or type are set, both must be populated").subject("Truststore Properties").build());
            }
        }

        return results;
    }

    /**
     * Returns a list of {@link ValidationResult}s when validating an actual JKS or PKCS12 file on disk. Verifies the
     * file permissions and existence, and attempts to open the file given the provided password.
     *
     * @param filename     the path of the file on disk
     * @param password     the file password
     * @param type         the type (JKS or PKCS12)
     * @return the list of validation results (empty is valid)
     */
    private static List<ValidationResult> validateTruststoreFile(String filename, String password, String type) {
        List<ValidationResult> results = new ArrayList<>();

        final File file = new File(filename);
        if (!file.exists() || !file.canRead()) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject("Truststore Properties")
                    .explanation("Cannot access file " + file.getAbsolutePath())
                    .build());
        } else {
            char[] passwordChars = new char[0];
            if (!StringUtils.isBlank(password)) {
                passwordChars = password.toCharArray();
            }
            try {
                final boolean storeValid = KeyStoreUtils.isStoreValid(file.toURI().toURL(), KeystoreType.valueOf(type), passwordChars);
                if (!storeValid) {
                    results.add(new ValidationResult.Builder()
                            .subject("Truststore Properties")
                            .valid(false)
                            .explanation("Invalid truststore password or type specified for file " + filename)
                            .build());
                }

            } catch (MalformedURLException e) {
                results.add(new ValidationResult.Builder()
                        .subject("Truststore Properties")
                        .valid(false)
                        .explanation("Malformed URL from file: " + e)
                        .build());
            }
        }

        return results;
    }

    /**
     * Returns a list of {@link ValidationResult}s when validating an actual JKS or PKCS12 file on disk. Verifies the
     * file permissions and existence, and attempts to open the file given the provided (keystore or key) password.
     *
     * @param filename     the path of the file on disk
     * @param password     the file password
     * @param keyPassword  the (optional) key-specific password
     * @param type         the type (JKS or PKCS12)
     * @return the list of validation results (empty is valid)
     */
    private static List<ValidationResult> validateKeystoreFile(String filename, String password, String keyPassword, String type) {
        List<ValidationResult> results = new ArrayList<>();

        final File file = new File(filename);
        if (!file.exists() || !file.canRead()) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject("Keystore Properties")
                    .explanation("Cannot access file " + file.getAbsolutePath())
                    .build());
        } else {
            char[] passwordChars = new char[0];
            if (!StringUtils.isBlank(password)) {
                passwordChars = password.toCharArray();
            }
            try {
                final boolean storeValid = KeyStoreUtils.isStoreValid(file.toURI().toURL(), KeystoreType.valueOf(type), passwordChars);
                if (!storeValid) {
                    results.add(new ValidationResult.Builder()
                            .subject("Keystore Properties")
                            .valid(false)
                            .explanation("Invalid keystore password or type specified for file " + filename)
                            .build());
                }

                // The key password can be explicitly set (and can be the same as the
                // keystore password or different), or it can be left blank. In the event
                // it's blank, the keystore password will be used
                char[] keyPasswordChars = new char[0];
                if (StringUtils.isBlank(keyPassword) || keyPassword.equals(password)) {
                    keyPasswordChars = passwordChars;
                }
                if (!StringUtils.isBlank(keyPassword)) {
                    keyPasswordChars = keyPassword.toCharArray();
                }

                boolean keyPasswordValid = KeyStoreUtils.isKeyPasswordCorrect(file.toURI().toURL(), KeystoreType.valueOf(type), passwordChars, keyPasswordChars);
                if (!keyPasswordValid) {
                    results.add(new ValidationResult.Builder()
                            .subject("Keystore Properties")
                            .valid(false)
                            .explanation("Invalid key password specified for file " + filename)
                            .build());
                }

            } catch (MalformedURLException e) {
                results.add(new ValidationResult.Builder()
                        .subject("Keystore Properties")
                        .valid(false)
                        .explanation("Malformed URL from file: " + e)
                        .build());
            }
        }

        return results;
    }

    public enum KeystoreValidationGroup {

        KEYSTORE, TRUSTSTORE
    }

    @Override
    public String toString() {
        return "SSLContextService[id=" + getIdentifier() + "]";
    }
}
