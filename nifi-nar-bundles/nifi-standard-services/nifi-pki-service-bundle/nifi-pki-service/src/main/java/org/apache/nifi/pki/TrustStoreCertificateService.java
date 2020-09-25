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
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Trust Store Certificate Service implementation finds X.509 Certificates with matching Subject Principal
 */
@Tags({"PKI", "X.509", "Certificates"})
@CapabilityDescription("Certificate Service providing X.509 Certificates from Trust Store files")
public class TrustStoreCertificateService extends AbstractControllerService implements CertificateService {

    public static final PropertyDescriptor TRUST_STORE_PATH = new PropertyDescriptor.Builder()
            .name("Trust Store Path")
            .displayName("Trust Store Path")
            .description("File path for Trust Store")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TRUST_STORE_TYPE = new PropertyDescriptor.Builder()
            .name("Trust Store Type")
            .displayName("Trust Store Type")
            .description("Type of Trust Store supports either JKS or PKCS12")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("PKCS12")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TRUST_STORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("Trust Store Password")
            .displayName("Trust Store Password")
            .description("Password for Trust Store")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = new ArrayList<>();

    private static final String DEFAULT_SEARCH = ".*";

    private static final String TRUST_STORE_LOAD_FAILED = "Trust Store [%s] loading failed";

    private static final String FIND_FAILED = "Find Certificates Search [%s] failed";

    static {
        DESCRIPTORS.add(TRUST_STORE_PATH);
        DESCRIPTORS.add(TRUST_STORE_TYPE);
        DESCRIPTORS.add(TRUST_STORE_PASSWORD);
    }

    private KeyStore trustStore;

    /**
     * On Enabled configures Trust Store using Context properties
     *
     * @param context Configuration Context with properties
     * @throws InitializationException Thrown when unable to load Trust Store
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String trustStoreType = context.getProperty(TRUST_STORE_TYPE).evaluateAttributeExpressions().getValue();
        final String trustStorePath = context.getProperty(TRUST_STORE_PATH).evaluateAttributeExpressions().getValue();
        char[] password = null;
        final PropertyValue passwordProperty = context.getProperty(TRUST_STORE_PASSWORD);
        if (passwordProperty.isSet()) {
            password = passwordProperty.getValue().toCharArray();
        }

        try {
            trustStore = KeyStoreUtils.loadTrustStore(trustStorePath, password, trustStoreType);
        } catch (final TlsException e) {
            final String message = String.format(TRUST_STORE_LOAD_FAILED, trustStorePath);
            throw new InitializationException(message, e);
        }
    }

    /**
     * Find X.509 Certificates in Trust Store where Subject Principal matches provided search pattern
     *
     * @param search Search String
     * @return Matching X.509 Certificates
     */
    @Override
    public List<X509Certificate> findCertificates(final String search) {
        final Pattern searchPattern = getSearchPattern(search);
        try {
            return findMatchingCertificates(searchPattern);
        } catch (final KeyStoreException e) {
            final String message = String.format(FIND_FAILED, search);
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
     * Get Search Pattern converts null to wildcard pattern and sets case insensitive matching
     *
     * @param search Search parameter can be null
     * @return Search Pattern
     */
    private Pattern getSearchPattern(final String search) {
        final String searchExpression = search == null ? DEFAULT_SEARCH : search;
        return Pattern.compile(searchExpression, Pattern.CASE_INSENSITIVE);
    }

    /**
     * Find Certificates with Subject Principal matching Search Pattern
     *
     * @param searchPattern Search Pattern for matching against Subject Principals
     * @return Certificates found
     * @throws KeyStoreException Thrown on KeyStore.getCertificate()
     */
    private List<X509Certificate> findMatchingCertificates(final Pattern searchPattern) throws KeyStoreException {
        final List<X509Certificate> certificates = new ArrayList<>();

        final Enumeration<String> aliases = trustStore.aliases();
        while (aliases.hasMoreElements()) {
            final String alias = aliases.nextElement();
            final Certificate certificate = trustStore.getCertificate(alias);
            if (certificate instanceof X509Certificate) {
                final X509Certificate trustedCertificate = (X509Certificate) certificate;
                final X500Principal subjectPrincipal = trustedCertificate.getSubjectX500Principal();
                final String subject = subjectPrincipal.toString();
                final Matcher subjectMatcher = searchPattern.matcher(subject);
                if (subjectMatcher.find()) {
                    certificates.add(trustedCertificate);
                }
            }
        }

        return certificates;
    }
}
