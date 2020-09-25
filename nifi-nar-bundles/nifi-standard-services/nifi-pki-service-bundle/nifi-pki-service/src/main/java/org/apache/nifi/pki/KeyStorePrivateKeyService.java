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
package org.apache.nifi.pki;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TlsException;

import javax.security.auth.x500.X500Principal;
import java.math.BigInteger;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;

/**
 * Private Key Service implementation uses a configured Key Store to find Private Keys matching Certificate properties
 */
@Tags({"PKI", "X.509", "Certificates"})
@CapabilityDescription("Private Key Service providing Private Keys matching X.509 Certificates from Key Store files")
public class KeyStorePrivateKeyService extends AbstractControllerService implements PrivateKeyService {
    public static final PropertyDescriptor KEY_STORE_PATH = new PropertyDescriptor.Builder()
            .name("Key Store Path")
            .displayName("Key Store Path")
            .description("File path for Key Store")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_STORE_TYPE = new PropertyDescriptor.Builder()
            .name("Key Store Type")
            .displayName("Key Store Type")
            .description("Type of Key Store supports either JKS or PKCS12")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("PKCS12")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_STORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("Key Store Password")
            .displayName("Key Store Password")
            .description("Password for Key Store")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_PASSWORD = new PropertyDescriptor.Builder()
            .name("Key Password")
            .displayName("Key Password")
            .description("Password for Private Key Entry in Key Store")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final String KEY_STORE_LOAD_FAILED = "Key Store [%s] loading failed";

    private static final String FIND_FAILED = "Find Private Key for Serial Number [%s] Issuer [%s] failed";

    private static final List<PropertyDescriptor> DESCRIPTORS = new ArrayList<>();

    static {
        DESCRIPTORS.add(KEY_STORE_PATH);
        DESCRIPTORS.add(KEY_STORE_TYPE);
        DESCRIPTORS.add(KEY_STORE_PASSWORD);
        DESCRIPTORS.add(KEY_PASSWORD);
    }

    private KeyStore keyStore;

    private char[] keyPassword;

    /**
     * On Enabled configures Trust Store using Context properties
     *
     * @param context Configuration Context with properties
     * @throws InitializationException Thrown when unable to load Trust Store
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String keyStoreType = context.getProperty(KEY_STORE_TYPE).evaluateAttributeExpressions().getValue();
        final String keyStorePath = context.getProperty(KEY_STORE_PATH).evaluateAttributeExpressions().getValue();
        char[] password = null;
        final PropertyValue passwordProperty = context.getProperty(KEY_STORE_PASSWORD);
        if (passwordProperty.isSet()) {
            password = passwordProperty.getValue().toCharArray();
        }

        try {
            keyStore = KeyStoreUtils.loadKeyStore(keyStorePath, password, keyStoreType);
        } catch (final TlsException e) {
            final String message = String.format(KEY_STORE_LOAD_FAILED, keyStorePath);
            throw new InitializationException(message, e);
        }

        final PropertyValue keyPasswordProperty = context.getProperty(KEY_PASSWORD);
        if (keyPasswordProperty.isSet()) {
            keyPassword = keyPasswordProperty.getValue().toCharArray();
        }
    }

    /**
     * Find Private Key with X.509 Certificate matching Serial Number and Issuer
     *
     * @param serialNumber Certificate Serial Number
     * @param issuer       X.500 Principal of Certificate Issuer
     * @return Private Key
     */
    @Override
    public Optional<PrivateKey> findPrivateKey(final BigInteger serialNumber, final X500Principal issuer) {
        try {
            return findMatchingPrivateKey(serialNumber, issuer);
        } catch (final KeyStoreException | UnrecoverableKeyException | NoSuchAlgorithmException e) {
            final String message = String.format(FIND_FAILED, serialNumber, issuer);
            throw new ProcessException(message, e);
        }
    }

    /**
     * Get Supported Property Descriptors
     *
     * @return Supported Property Descriptors configured during initialization
     */
    @Override
    protected final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    /**
     * Find Matching Private Key
     *
     * @param serialNumber Certificate Serial Number
     * @param issuer       Certificate Issuer
     * @return Optional Private Key
     * @throws KeyStoreException         Thrown on KeyStore.aliases()
     * @throws UnrecoverableKeyException Thrown on KeyStore.getKey()
     * @throws NoSuchAlgorithmException  Thrown on KeyStore.getKey()
     */
    private Optional<PrivateKey> findMatchingPrivateKey(final BigInteger serialNumber, final X500Principal issuer) throws KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException {
        Optional<PrivateKey> matchingPrivateKey = Optional.empty();
        final Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            final String alias = aliases.nextElement();
            if (isCertificateMatched(alias, serialNumber, issuer)) {
                final Key key = keyStore.getKey(alias, keyPassword);
                if (key instanceof PrivateKey) {
                    final PrivateKey privateKey = (PrivateKey) key;
                    matchingPrivateKey = Optional.of(privateKey);
                }
            }
        }
        return matchingPrivateKey;
    }

    /**
     * Is Certificate Matched
     *
     * @param alias        Certificate Alias
     * @param serialNumber Certificate Serial Number to find
     * @param issuer       Certificate Issuer to find
     * @return Certificate Matched status
     * @throws KeyStoreException Thrown on KeyStore.getCertificate()
     */
    private boolean isCertificateMatched(final String alias, final BigInteger serialNumber, final X500Principal issuer) throws KeyStoreException {
        boolean matched = false;

        final Certificate certificateEntry = keyStore.getCertificate(alias);
        if (certificateEntry instanceof X509Certificate) {
            final X509Certificate certificate = (X509Certificate) certificateEntry;
            final BigInteger certificateSerialNumber = certificate.getSerialNumber();
            final X500Principal certificateIssuer = certificate.getIssuerX500Principal();
            matched = certificateSerialNumber.equals(serialNumber) && certificateIssuer.equals(issuer);
        }

        return matched;
    }
}
