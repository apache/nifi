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
package org.apache.nifi.processor.util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.SslContextFactory.ClientAuth;

public class SSLProperties {

    public static final PropertyDescriptor TRUSTSTORE = new PropertyDescriptor.Builder()
            .name("Truststore Filename")
            .description("The fully-qualified filename of the Truststore")
            .defaultValue(null)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor TRUSTSTORE_TYPE = new PropertyDescriptor.Builder()
            .name("Truststore Type")
            .description("The Type of the Truststore. Either JKS or PKCS12")
            .allowableValues("JKS", "PKCS12")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(null)
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
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor KEYSTORE_TYPE = new PropertyDescriptor.Builder()
            .name("Keystore Type")
            .description("The Type of the Keystore")
            .allowableValues("JKS", "PKCS12")
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

    public static Collection<ValidationResult> validateStore(final Map<PropertyDescriptor, String> properties) {
        final Collection<ValidationResult> results = new ArrayList<>();
        results.addAll(validateStore(properties, KeystoreValidationGroup.KEYSTORE));
        results.addAll(validateStore(properties, KeystoreValidationGroup.TRUSTSTORE));
        return results;
    }

    public static Collection<ValidationResult> validateStore(final Map<PropertyDescriptor, String> properties, final KeystoreValidationGroup keyStoreOrTrustStore) {
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
            results.add(new ValidationResult.Builder().valid(false).explanation("Must set either 0 or 3 properties for " + keystoreDesc).subject(keystoreDesc + " Properties").build());
        } else if (nulls == 0) {
            // all properties were filled in.
            final File file = new File(filename);
            if (!file.exists() || !file.canRead()) {
                results.add(new ValidationResult.Builder().valid(false).subject(keystoreDesc + " Properties").explanation("Cannot access file " + file.getAbsolutePath()).build());
            } else {
                try {
                    final boolean storeValid = CertificateUtils.isStoreValid(file.toURI().toURL(), KeystoreType.valueOf(type), password.toCharArray());
                    if (!storeValid) {
                        results.add(new ValidationResult.Builder().subject(keystoreDesc + " Properties").valid(false).explanation("Invalid KeyStore Password or Type specified for file " + filename).build());
                    }
                } catch (MalformedURLException e) {
                    results.add(new ValidationResult.Builder().subject(keystoreDesc + " Properties").valid(false).explanation("Malformed URL from file: " + e).build());
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

    public static List<PropertyDescriptor> getKeystoreDescriptors(final boolean required) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        for (final PropertyDescriptor descriptor : KEYSTORE_DESCRIPTORS) {
            final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder().fromPropertyDescriptor(descriptor).required(required);
            if (required && descriptor.getName().equals(KEYSTORE_TYPE.getName())) {
                builder.defaultValue("JKS");
            }
            descriptors.add(builder.build());
        }

        return descriptors;
    }

    public static List<PropertyDescriptor> getTruststoreDescriptors(final boolean required) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        for (final PropertyDescriptor descriptor : TRUSTSTORE_DESCRIPTORS) {
            final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder().fromPropertyDescriptor(descriptor).required(required);
            if (required && descriptor.getName().equals(TRUSTSTORE_TYPE.getName())) {
                builder.defaultValue("JKS");
            }
            descriptors.add(builder.build());
        }

        return descriptors;
    }

    public static SSLContext createSSLContext(final ProcessContext context, final ClientAuth clientAuth)
            throws UnrecoverableKeyException, KeyManagementException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        final String keystoreFile = context.getProperty(KEYSTORE).getValue();
        if (keystoreFile == null) {
            return SslContextFactory.createTrustSslContext(
                    context.getProperty(TRUSTSTORE).getValue(),
                    context.getProperty(TRUSTSTORE_PASSWORD).getValue().toCharArray(),
                    context.getProperty(TRUSTSTORE_TYPE).getValue());
        } else {
            final String truststoreFile = context.getProperty(TRUSTSTORE).getValue();
            if (truststoreFile == null) {
                return SslContextFactory.createSslContext(
                        context.getProperty(KEYSTORE).getValue(),
                        context.getProperty(KEYSTORE_PASSWORD).getValue().toCharArray(),
                        context.getProperty(KEYSTORE_TYPE).getValue());
            } else {
                return SslContextFactory.createSslContext(
                        context.getProperty(KEYSTORE).getValue(),
                        context.getProperty(KEYSTORE_PASSWORD).getValue().toCharArray(),
                        context.getProperty(KEYSTORE_TYPE).getValue(),
                        context.getProperty(TRUSTSTORE).getValue(),
                        context.getProperty(TRUSTSTORE_PASSWORD).getValue().toCharArray(),
                        context.getProperty(TRUSTSTORE_TYPE).getValue(),
                        clientAuth);
            }
        }
    }

    private static final Set<PropertyDescriptor> KEYSTORE_DESCRIPTORS = new HashSet<>();
    private static final Set<PropertyDescriptor> TRUSTSTORE_DESCRIPTORS = new HashSet<>();

    static {
        KEYSTORE_DESCRIPTORS.add(KEYSTORE);
        KEYSTORE_DESCRIPTORS.add(KEYSTORE_TYPE);
        KEYSTORE_DESCRIPTORS.add(KEYSTORE_PASSWORD);

        TRUSTSTORE_DESCRIPTORS.add(TRUSTSTORE);
        TRUSTSTORE_DESCRIPTORS.add(TRUSTSTORE_TYPE);
        TRUSTSTORE_DESCRIPTORS.add(TRUSTSTORE_PASSWORD);
    }
}
