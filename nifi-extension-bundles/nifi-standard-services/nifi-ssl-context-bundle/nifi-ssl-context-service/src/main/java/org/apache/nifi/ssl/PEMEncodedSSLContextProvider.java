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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.ssl.BuilderConfigurationException;
import org.apache.nifi.security.ssl.PemCertificateKeyStoreBuilder;
import org.apache.nifi.security.ssl.PemPrivateKeyCertificateKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardKeyManagerBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.apache.nifi.security.util.TlsPlatform;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Tags({"PEM", "SSL", "TLS", "Key", "Certificate", "PKCS1", "PKCS8", "X.509", "ECDSA", "Ed25519", "RSA"})
@CapabilityDescription("""
    SSLContext Provider configurable using PEM Private Key and Certificate files.
    Supports PKCS1 and PKCS8 encoding for Private Keys as well as X.509 encoding for Certificates.
""")
public class PEMEncodedSSLContextProvider extends AbstractControllerService implements SSLContextProvider, VerifiableControllerService {
    static final String DEFAULT_PROTOCOL = "TLS";

    static final PropertyDescriptor TLS_PROTOCOL = new PropertyDescriptor.Builder()
            .name("TLS Protocol")
            .description("TLS protocol version required for negotiating encrypted communications.")
            .required(true)
            .sensitive(false)
            .defaultValue(DEFAULT_PROTOCOL)
            .allowableValues(getProtocolAllowableValues())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PRIVATE_KEY_SOURCE = new PropertyDescriptor.Builder()
            .name("Private Key Source")
            .description("Source of information for loading Private Key and Certificate Chain")
            .required(true)
            .defaultValue(PrivateKeySource.PROPERTIES)
            .allowableValues(PrivateKeySource.class)
            .build();

    static final PropertyDescriptor PRIVATE_KEY = new PropertyDescriptor.Builder()
            .name("Private Key")
            .description("PEM Private Key encoded using either PKCS1 or PKCS8. Supported algorithms include ECDSA, Ed25519, and RSA")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.TEXT)
            .dependsOn(PRIVATE_KEY_SOURCE, PrivateKeySource.PROPERTIES)
            .build();

    static final PropertyDescriptor PRIVATE_KEY_LOCATION = new PropertyDescriptor.Builder()
            .name("Private Key Location")
            .description("PEM Private Key file location encoded using either PKCS1 or PKCS8. Supported algorithms include ECDSA, Ed25519, and RSA")
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .dependsOn(PRIVATE_KEY_SOURCE, PrivateKeySource.FILES)
            .build();

    static final PropertyDescriptor CERTIFICATE_CHAIN = new PropertyDescriptor.Builder()
            .name("Certificate Chain")
            .description("PEM X.509 Certificate Chain associated with Private Key starting with standard BEGIN CERTIFICATE header")
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.TEXT)
            .dependsOn(PRIVATE_KEY_SOURCE, PrivateKeySource.PROPERTIES)
            .build();

    static final PropertyDescriptor CERTIFICATE_CHAIN_LOCATION = new PropertyDescriptor.Builder()
            .name("Certificate Chain Location")
            .description("PEM X.509 Certificate Chain file location associated with Private Key starting with standard BEGIN CERTIFICATE header")
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .dependsOn(PRIVATE_KEY_SOURCE, PrivateKeySource.FILES)
            .build();

    static final PropertyDescriptor CERTIFICATE_AUTHORITIES_SOURCE = new PropertyDescriptor.Builder()
            .name("Certificate Authorities Source")
            .description("Source of information for loading trusted Certificate Authorities")
            .required(true)
            .defaultValue(CertificateAuthoritiesSource.PROPERTIES)
            .allowableValues(CertificateAuthoritiesSource.class)
            .build();

    static final PropertyDescriptor CERTIFICATE_AUTHORITIES = new PropertyDescriptor.Builder()
            .name("Certificate Authorities")
            .description("PEM X.509 Certificate Authorities trusted for verifying peers in TLS communications containing one or more standard certificates")
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.TEXT)
            .dependsOn(CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.PROPERTIES)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            TLS_PROTOCOL,
            PRIVATE_KEY_SOURCE,
            PRIVATE_KEY,
            PRIVATE_KEY_LOCATION,
            CERTIFICATE_CHAIN,
            CERTIFICATE_CHAIN_LOCATION,
            CERTIFICATE_AUTHORITIES_SOURCE,
            CERTIFICATE_AUTHORITIES
    );

    private static final char[] EMPTY_PROTECTION_PARAMETER = new char[]{};

    private String protocol = DEFAULT_PROTOCOL;

    private KeyStore keyStore;

    private KeyStore trustStore;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        final ConfigVerificationResult.Builder privateKeyBuilder = new ConfigVerificationResult.Builder().verificationStepName("Load Private Key and Certificate Chain");
        final PrivateKeySource privateKeySource = context.getProperty(PRIVATE_KEY_SOURCE).asAllowableValue(PrivateKeySource.class);
        if (privateKeySource == PrivateKeySource.UNDEFINED) {
            privateKeyBuilder.outcome(Outcome.SKIPPED).explanation("Private Key and Certificate Chain properties not required");
        } else {
            try {
                loadKeyStore(context);
                privateKeyBuilder.outcome(Outcome.SUCCESSFUL);
            } catch (final Exception e) {
                privateKeyBuilder.outcome(Outcome.FAILED).explanation(e.getMessage());
            }
        }
        results.add(privateKeyBuilder.build());

        final ConfigVerificationResult.Builder authoritiesBuilder = new ConfigVerificationResult.Builder().verificationStepName("Load Certificate Authorities");
        try {
            loadTrustStore(context);
            authoritiesBuilder.outcome(Outcome.SUCCESSFUL);
        } catch (final Exception e) {
            authoritiesBuilder.outcome(Outcome.FAILED).explanation(e.getMessage());
        }
        results.add(authoritiesBuilder.build());

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        protocol = context.getProperty(TLS_PROTOCOL).getValue();
        loadKeyStore(context);
        loadTrustStore(context);
    }

    @OnDisabled
    public void onDisabled() {
        keyStore = null;
        trustStore = null;
    }

    @Override
    public SSLContext createContext() {
        final StandardSslContextBuilder sslContextBuilder = new StandardSslContextBuilder();
        sslContextBuilder.protocol(protocol);

        final X509TrustManager trustManager = createTrustManager();
        sslContextBuilder.trustManager(trustManager);

        final Optional<X509ExtendedKeyManager> keyManagerCreated = createKeyManager();
        if (keyManagerCreated.isPresent()) {
            final X509ExtendedKeyManager keyManager = keyManagerCreated.get();
            sslContextBuilder.keyManager(keyManager);
        }

        return sslContextBuilder.build();
    }

    @Override
    public Optional<X509ExtendedKeyManager> createKeyManager() {
        final Optional<X509ExtendedKeyManager> keyManagerCreated;

        if (keyStore == null) {
            keyManagerCreated = Optional.empty();
        } else {
            final X509ExtendedKeyManager keyManager = new StandardKeyManagerBuilder()
                    .keyStore(keyStore)
                    .keyPassword(EMPTY_PROTECTION_PARAMETER)
                    .build();
            keyManagerCreated = Optional.of(keyManager);
        }

        return keyManagerCreated;
    }

    @Override
    public X509TrustManager createTrustManager() {
        final X509ExtendedTrustManager trustManager;

        if (trustStore == null) {
            try {
                final String algorithm = TrustManagerFactory.getDefaultAlgorithm();
                final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(algorithm);
                trustManagerFactory.init(trustStore);

                final TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
                final Optional<X509ExtendedTrustManager> configuredTrustManager = Arrays.stream(trustManagers)
                        .filter(manager -> manager instanceof X509ExtendedTrustManager)
                        .map(manager -> (X509ExtendedTrustManager) manager)
                        .findFirst();
                trustManager = configuredTrustManager.orElseThrow(() -> new BuilderConfigurationException("X.509 Trust Manager not configured"));
            } catch (final GeneralSecurityException e) {
                throw new BuilderConfigurationException("Trust Manager creation failed for System Certificate Authorities", e);
            }
        } else {
            trustManager = new StandardTrustManagerBuilder().trustStore(trustStore).build();
        }

        return trustManager;
    }

    private void loadKeyStore(final ConfigurationContext context) throws InitializationException {
        final PrivateKeySource privateKeySource = context.getProperty(PRIVATE_KEY_SOURCE).asAllowableValue(PrivateKeySource.class);
        if (privateKeySource == PrivateKeySource.UNDEFINED) {
            getLogger().debug("Private Key and Certificate Chain not configured");
        } else {
            final PropertyDescriptor privateKeyProperty;
            final PropertyDescriptor certificateChainProperty;

            if (privateKeySource == PrivateKeySource.FILES) {
                privateKeyProperty = PRIVATE_KEY_LOCATION;
                certificateChainProperty = CERTIFICATE_CHAIN_LOCATION;
            } else {
                privateKeyProperty = PRIVATE_KEY;
                certificateChainProperty = CERTIFICATE_CHAIN;
            }

            final ResourceReference privateKeyReference = context.getProperty(privateKeyProperty).asResource();
            final ResourceReference certificateChainReference = context.getProperty(certificateChainProperty).asResource();
            final PemPrivateKeyCertificateKeyStoreBuilder keyStoreBuilder = new PemPrivateKeyCertificateKeyStoreBuilder();
            try (
                    InputStream privateKeyInputStream = privateKeyReference.read();
                    InputStream certificateInputStream = certificateChainReference.read()
            ) {
                keyStore = keyStoreBuilder
                        .privateKeyInputStream(privateKeyInputStream)
                        .certificateInputStream(certificateInputStream)
                        .build();
            } catch (final Exception e) {
                throw new InitializationException("Failed to load Private Key or Certificate Chain from configured properties", e);
            }
        }
    }

    private void loadTrustStore(final ConfigurationContext context) throws InitializationException {
        final CertificateAuthoritiesSource certificateAuthoritiesSource = context.getProperty(CERTIFICATE_AUTHORITIES_SOURCE).asAllowableValue(CertificateAuthoritiesSource.class);
        if (certificateAuthoritiesSource == CertificateAuthoritiesSource.SYSTEM) {
            trustStore = null;
        } else if (certificateAuthoritiesSource == CertificateAuthoritiesSource.PROPERTIES) {
            final ResourceReference certificateAuthoritiesReference = context.getProperty(CERTIFICATE_AUTHORITIES).asResource();
            try (InputStream certificateAuthoritiesStream = certificateAuthoritiesReference.read()) {
                trustStore = new PemCertificateKeyStoreBuilder().inputStream(certificateAuthoritiesStream).build();
            } catch (final Exception e) {
                throw new InitializationException("Failed to load Certificate Authorities from configured properties", e);
            }
        }
    }

    private static AllowableValue[] getProtocolAllowableValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();

        allowableValues.add(new AllowableValue(DEFAULT_PROTOCOL, DEFAULT_PROTOCOL, "Negotiate latest TLS protocol version based on platform supported versions"));

        for (final String supportedProtocol : TlsPlatform.getPreferredProtocols()) {
            final String description = String.format("Require %s protocol version", supportedProtocol);
            allowableValues.add(new AllowableValue(supportedProtocol, supportedProtocol, description));
        }

        return allowableValues.toArray(new AllowableValue[0]);
    }

    enum PrivateKeySource implements DescribedValue {
        UNDEFINED("Undefined", "Avoid configuring Private Key and Certificate Chain properties"),
        PROPERTIES("Properties", "Load Private Key and Certificate Chain from configured properties"),
        FILES("Files", "Load Private Key and Certificate Chain from configured files");

        private final String displayName;

        private final String description;

        PrivateKeySource(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    enum CertificateAuthoritiesSource implements DescribedValue {
        PROPERTIES("Properties", "Load trusted Certificate Authorities from configured properties"),
        SYSTEM("System", "Load trusted Certificate Authorities from the default system location");

        private final String displayName;

        private final String description;

        CertificateAuthoritiesSource(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }
}
