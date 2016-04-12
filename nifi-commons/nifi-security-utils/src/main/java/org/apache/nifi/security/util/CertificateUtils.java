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
package org.apache.nifi.security.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CertificateUtils {

    private static final Logger logger = LoggerFactory.getLogger(CertificateUtils.class);

    /**
     * Returns true if the given keystore can be loaded using the given keystore type and password. Returns false otherwise.
     *
     * @param keystore     the keystore to validate
     * @param keystoreType the type of the keystore
     * @param password     the password to access the keystore
     * @return true if valid; false otherwise
     */
    public static boolean isStoreValid(final URL keystore, final KeystoreType keystoreType, final char[] password) {

        if (keystore == null) {
            throw new IllegalArgumentException("keystore may not be null");
        } else if (keystoreType == null) {
            throw new IllegalArgumentException("keystore type may not be null");
        } else if (password == null) {
            throw new IllegalArgumentException("password may not be null");
        }

        BufferedInputStream bis = null;
        final KeyStore ks;
        try {

            // load the keystore
            bis = new BufferedInputStream(keystore.openStream());
            ks = KeyStore.getInstance(keystoreType.name());
            ks.load(bis, password);

            return true;

        } catch (Exception e) {
            return false;
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close input stream", ioe);
                }
            }
        }
    }

    /**
     * Extracts the username from the specified DN. If the username cannot be extracted because the CN is in an unrecognized format, the entire CN is returned. If the CN cannot be extracted because
     * the DN is in an unrecognized format, the entire DN is returned.
     *
     * @param dn the dn to extract the username from
     * @return the exatracted username
     */
    public static String extractUsername(String dn) {
        String username = dn;

        // ensure the dn is specified
        if (StringUtils.isNotBlank(dn)) {
            // determine the separate
            final String separator = StringUtils.indexOfIgnoreCase(dn, "/cn=") > 0 ? "/" : ",";

            // attempt to locate the cd
            final String cnPattern = "cn=";
            final int cnIndex = StringUtils.indexOfIgnoreCase(dn, cnPattern);
            if (cnIndex >= 0) {
                int separatorIndex = StringUtils.indexOf(dn, separator, cnIndex);
                if (separatorIndex > 0) {
                    username = StringUtils.substring(dn, cnIndex + cnPattern.length(), separatorIndex);
                } else {
                    username = StringUtils.substring(dn, cnIndex + cnPattern.length());
                }
            }
        }

        return username;
    }

    /**
     * Returns a list of subject alternative names. Any name that is represented as a String by X509Certificate.getSubjectAlternativeNames() is converted to lowercase and returned.
     *
     * @param certificate a certificate
     * @return a list of subject alternative names; list is never null
     * @throws CertificateParsingException if parsing the certificate failed
     */
    public static List<String> getSubjectAlternativeNames(final X509Certificate certificate) throws CertificateParsingException {

        final Collection<List<?>> altNames = certificate.getSubjectAlternativeNames();
        if (altNames == null) {
            return new ArrayList<>();
        }

        final List<String> result = new ArrayList<>();
        for (final List<?> generalName : altNames) {
            /**
             * generalName has the name type as the first element a String or byte array for the second element. We return any general names that are String types.
             *
             * We don't inspect the numeric name type because some certificates incorrectly put IPs and DNS names under the wrong name types.
             */
            final Object value = generalName.get(1);
            if (value instanceof String) {
                result.add(((String) value).toLowerCase());
            }

        }

        return result;
    }

    public static String extractClientDNFromSSLSocket(Socket socket) throws CertificateException {
        String dn = null;
        if (socket instanceof SSLSocket) {
            final SSLSocket sslSocket = (SSLSocket) socket;
            try {
                final Certificate[] certChains = sslSocket.getSession().getPeerCertificates();
                if (certChains != null && certChains.length > 0) {
                    X509Certificate x509Certificate = convertAbstractX509Certificate(certChains[0]);
                    dn = x509Certificate.getSubjectDN().getName().trim();
                }
            } catch (SSLPeerUnverifiedException e) {
                throw new CertificateException(e);
            }
        }

        return dn;
    }

    /**
     * Accepts a legacy {@link javax.security.cert.X509Certificate} and returns an {@link X509Certificate}. The {@code javax.*} package certificate classes are for legacy compatibility and should
     * not be used for new development.
     *
     * @param legacyCertificate the {@code javax.security.cert.X509Certificate}
     * @return a new {@code java.security.cert.X509Certificate}
     * @throws CertificateException if there is an error generating the new certificate
     */
    public static X509Certificate convertLegacyX509Certificate(javax.security.cert.X509Certificate legacyCertificate) throws CertificateException {
        if (legacyCertificate == null) {
            throw new IllegalArgumentException("The X.509 certificate cannot be null");
        }

        try {
            return formX509Certificate(legacyCertificate.getEncoded());
        } catch (javax.security.cert.CertificateEncodingException e) {
            throw new CertificateException(e);
        }
    }

    /**
     * Accepts an abstract {@link java.security.cert.Certificate} and returns an {@link X509Certificate}. Because {@code sslSocket.getSession().getPeerCertificates()} returns an array of the
     * abstract certificates, they must be translated to X.509 to replace the functionality of {@code sslSocket.getSession().getPeerCertificateChain()}.
     *
     * @param abstractCertificate the {@code java.security.cert.Certificate}
     * @return a new {@code java.security.cert.X509Certificate}
     * @throws CertificateException if there is an error generating the new certificate
     */
    public static X509Certificate convertAbstractX509Certificate(java.security.cert.Certificate abstractCertificate) throws CertificateException {
        if (abstractCertificate == null || !(abstractCertificate instanceof X509Certificate)) {
            throw new IllegalArgumentException("The certificate cannot be null and must be an X.509 certificate");
        }

        try {
            return formX509Certificate(abstractCertificate.getEncoded());
        } catch (java.security.cert.CertificateEncodingException e) {
            throw new CertificateException(e);
        }
    }

    private static X509Certificate formX509Certificate(byte[] encodedCertificate) throws CertificateException {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            ByteArrayInputStream bais = new ByteArrayInputStream(encodedCertificate);
            return (X509Certificate) cf.generateCertificate(bais);
        } catch (CertificateException e) {
            logger.error("Error converting the certificate", e);
            throw e;
        }
    }

    private CertificateUtils() {
    }
}
