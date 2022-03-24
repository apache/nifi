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

package org.apache.nifi.toolkit.tls.commandLine;

import org.apache.commons.cli.CommandLine;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class with common CLI parsing functionality as well as arguments shared by multiple entry points
 */
public abstract class BaseTlsToolkitCommandLine extends BaseCommandLine {
    private static final Logger logger = LoggerFactory.getLogger(BaseTlsToolkitCommandLine.class);

    public static final String KEY_SIZE_ARG = "keySize";
    public static final String KEY_ALGORITHM_ARG = "keyAlgorithm";
    public static final String CERTIFICATE_AUTHORITY_HOSTNAME_ARG = "certificateAuthorityHostname";
    public static final String DAYS_ARG = "days";
    public static final String KEY_STORE_TYPE_ARG = "keyStoreType";
    public static final String SIGNING_ALGORITHM_ARG = "signingAlgorithm";
    public static final String DN_ARG = "dn";
    public static final String DIFFERENT_KEY_AND_KEYSTORE_PASSWORDS_ARG = "differentKeyAndKeystorePasswords";

    public static final String KEYSTORE = "keystore.";
    public static final String TRUSTSTORE = "truststore.";

    private int keySize;
    private String keyAlgorithm;
    private String certificateAuthorityHostname;
    private String keyStoreType;
    private int days;
    private String signingAlgorithm;
    private boolean differentPasswordForKeyAndKeystore;

    public BaseTlsToolkitCommandLine(String header) {
        super(header);
        if (shouldAddDaysArg()) {
            addOptionWithArg("d", DAYS_ARG, "Number of days issued certificate should be valid for.", TlsConfig.DEFAULT_DAYS);
        }
        addOptionWithArg("T", KEY_STORE_TYPE_ARG, "The type of keyStores to generate.", getKeyStoreTypeDefault());
        addOptionWithArg("c", CERTIFICATE_AUTHORITY_HOSTNAME_ARG, "Hostname of NiFi Certificate Authority", TlsConfig.DEFAULT_HOSTNAME);
        addOptionWithArg("a", KEY_ALGORITHM_ARG, "Algorithm to use for generated keys.", TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);
        addOptionWithArg("k", KEY_SIZE_ARG, "Number of bits for generated keys.", TlsConfig.DEFAULT_KEY_SIZE);
        if (shouldAddSigningAlgorithmArg()) {
            addOptionWithArg("s", SIGNING_ALGORITHM_ARG, "Algorithm to use for signing certificates.", TlsConfig.DEFAULT_SIGNING_ALGORITHM);
        }
        addOptionNoArg("g", DIFFERENT_KEY_AND_KEYSTORE_PASSWORDS_ARG, "Use different generated password for the key and the keyStore.");
    }

    protected String getKeyStoreTypeDefault() {
        return TlsConfig.DEFAULT_KEY_STORE_TYPE;
    }

    protected boolean shouldAddSigningAlgorithmArg() {
        return true;
    }

    protected boolean shouldAddDaysArg() {
        return true;
    }

    /**
     * Returns the number of bits used when generating KeyPairs
     *
     * @return the number of bits used when generating KeyPairs
     */
    public int getKeySize() {
        return keySize;
    }

    /**
     * Returns the algorithm used when generating KeyPairs
     *
     * @return the algorithm used when generating KeyPairs
     */
    public String getKeyAlgorithm() {
        return keyAlgorithm;
    }

    /**
     * Returns the CA Hostname
     *
     * @return the CA Hostname
     */
    public String getCertificateAuthorityHostname() {
        return certificateAuthorityHostname;
    }

    /**
     * Returns the type to use for KeyStores
     *
     * @return the type to use for KeyStores
     */
    public String getKeyStoreType() {
        return keyStoreType;
    }

    /**
     * Returns the number of Certificates should be valid for
     *
     * @return the number of Certificates should be valid for
     */
    public int getDays() {
        return days;
    }

    /**
     * Returns the signing algorithm to use for cryptographic operations
     *
     * @return the signing algorithm to use for cryptographic operations
     */
    public String getSigningAlgorithm() {
        return signingAlgorithm;
    }

    /**
     * Returns true if different passwords should be used for KeyStore and individual Key entries
     *
     * @return true if different passwords should be used for KeyStore and individual Key entries
     */
    public boolean differentPasswordForKeyAndKeystore() {
        return differentPasswordForKeyAndKeystore;
    }

    @Override
    protected void postParse(CommandLine commandLine) throws CommandLineParseException {
        certificateAuthorityHostname = commandLine.getOptionValue(CERTIFICATE_AUTHORITY_HOSTNAME_ARG, TlsConfig.DEFAULT_HOSTNAME);
        days = getIntValue(commandLine, DAYS_ARG, TlsConfig.DEFAULT_DAYS);
        keySize = getIntValue(commandLine, KEY_SIZE_ARG, TlsConfig.DEFAULT_KEY_SIZE);
        keyAlgorithm = commandLine.getOptionValue(KEY_ALGORITHM_ARG, TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);
        keyStoreType = commandLine.getOptionValue(KEY_STORE_TYPE_ARG, getKeyStoreTypeDefault());
        if (KeystoreType.PKCS12.toString().equalsIgnoreCase(keyStoreType)) {
            logger.info("Command line argument --" + KEY_STORE_TYPE_ARG + "=" + keyStoreType + " only applies to keystore, recommended truststore type of " + KeystoreType.JKS.toString() +
                    " unaffected.");
        }
        signingAlgorithm = commandLine.getOptionValue(SIGNING_ALGORITHM_ARG, TlsConfig.DEFAULT_SIGNING_ALGORITHM);
        differentPasswordForKeyAndKeystore = commandLine.hasOption(DIFFERENT_KEY_AND_KEYSTORE_PASSWORDS_ARG);
    }

}
