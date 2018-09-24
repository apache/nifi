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

package org.apache.nifi.toolkit.tls.configuration;

import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.util.StringUtils;

/**
 * Configuration object for CA server
 */
public class TlsConfig {
    public static final String DEFAULT_HOSTNAME = "localhost";
    public static final String DEFAULT_KEY_STORE_TYPE = "jks";
    public static final int DEFAULT_PORT = 8443;
    public static final int DEFAULT_DAYS = 3 * 365;
    public static final int DEFAULT_KEY_SIZE = 2048;
    public static final String DEFAULT_KEY_PAIR_ALGORITHM = "RSA";
    public static final String DEFAULT_SIGNING_ALGORITHM = "SHA256WITHRSA";
    public static final String DEFAULT_DN_PREFIX = "CN=";
    public static final String DEFAULT_DN_SUFFIX = ", OU=NIFI";
    public static final boolean DEFAULT_REORDER_DN = true;

    private int days = DEFAULT_DAYS;
    private int keySize = DEFAULT_KEY_SIZE;
    private String keyPairAlgorithm = DEFAULT_KEY_PAIR_ALGORITHM;
    private String signingAlgorithm = DEFAULT_SIGNING_ALGORITHM;

    private String dn;
    private String domainAlternativeNames;
    private String keyStore;
    private String keyStoreType = DEFAULT_KEY_STORE_TYPE;
    private String keyStorePassword;
    private String keyPassword;
    private String token;
    private String caHostname = DEFAULT_HOSTNAME;
    private int port = DEFAULT_PORT;
    private String dnPrefix = DEFAULT_DN_PREFIX;
    private String dnSuffix = DEFAULT_DN_SUFFIX;
    private boolean reorderDn = DEFAULT_REORDER_DN;
    private String additionalCACertificate = "";

    public String calcDefaultDn(String hostname) {
        String dn = dnPrefix + hostname + dnSuffix;
        if (reorderDn) {
            return CertificateUtils.reorderDn(dn);
        }
        return dn;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getKeyStore() {
        return keyStore;
    }

    public void setKeyStore(String keyStore) {
        this.keyStore = keyStore;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getCaHostname() {
        return caHostname;
    }

    public void setCaHostname(String caHostname) {
        this.caHostname = caHostname;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    public int getDays() {
        return days;
    }

    public void setDays(int days) {
        this.days = days;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public String getKeyPairAlgorithm() {
        return keyPairAlgorithm;
    }

    public void setKeyPairAlgorithm(String keyPairAlgorithm) {
        this.keyPairAlgorithm = keyPairAlgorithm;
    }

    public String getSigningAlgorithm() {
        return signingAlgorithm;
    }

    public void setSigningAlgorithm(String signingAlgorithm) {
        this.signingAlgorithm = signingAlgorithm;
    }

    public String getDnPrefix() {
        return dnPrefix;
    }

    public void setDnPrefix(String dnPrefix) {
        this.dnPrefix = dnPrefix;
    }

    public String getDnSuffix() {
        return dnSuffix;
    }

    public void setDnSuffix(String dnSuffix) {
        this.dnSuffix = dnSuffix;
    }

    public boolean getReorderDn() {
        return reorderDn;
    }

    public void setReorderDn(boolean reorderDn) {
        this.reorderDn = reorderDn;
    }

    public void initDefaults() {
        if (days == 0) {
            days = DEFAULT_DAYS;
        }
        if (keySize == 0) {
            keySize = DEFAULT_KEY_SIZE;
        }
        if (StringUtils.isEmpty(keyPairAlgorithm)) {
            keyPairAlgorithm = DEFAULT_KEY_PAIR_ALGORITHM;
        }
        if (StringUtils.isEmpty(signingAlgorithm)) {
            signingAlgorithm = DEFAULT_SIGNING_ALGORITHM;
        }
        if (port == 0) {
            port = DEFAULT_PORT;
        }
        if (StringUtils.isEmpty(keyStoreType)) {
            keyStoreType = DEFAULT_KEY_STORE_TYPE;
        }
        if (StringUtils.isEmpty(caHostname)) {
            caHostname = DEFAULT_HOSTNAME;
        }
        if (StringUtils.isEmpty(dn)) {
            dn = calcDefaultDn(caHostname);
        }
    }

    public String getDomainAlternativeNames() {
        return domainAlternativeNames;
    }

    public void setDomainAlternativeNames(String domainAlternativeNames) {
        this.domainAlternativeNames = domainAlternativeNames;
    }

    /**
     * Returns the path to an additional CA certificate file in PEM format which has been used to sign the CA certificate the toolkit will use.
     *
     * Example:
     *
     * nifi-cert.pem [existing PEM file for intermediate CA generated by Org's IT team and signed by org-ca.pem]
     * org-ca.pem [PEM file for root CA owned by Org's IT team]
     *
     * {@code getAdditionalCACertificate() == "/path/to/org-ca.pem"}
     *
     * @return the path to this file
     */
    public String getAdditionalCACertificate() {
        return additionalCACertificate;
    }

    /**
     * Sets the path to an additional CA certificate file in PEM format which has been used to sign the CA certificate the toolkit will use.
     *
     * Example:
     *
     * nifi-cert.pem [existing PEM file for intermediate CA generated by Org's IT team and signed by org-ca.pem]
     * org-ca.pem [PEM file for root CA owned by Org's IT team]
     *
     * {@code setAdditionalCACertificate("/path/to/org-ca.pem");}
     *
     * @param additionalCACertificate the path to this file
     */
    public void setAdditionalCACertificate(String additionalCACertificate) {
        this.additionalCACertificate = additionalCACertificate;
    }
}
