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
package org.apache.nifi.properties.sensitive.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.SensitivePropertyConfigurationException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;

/**
 * HadoopCredentialsSensitivePropertyProvider gets values from a key store that is (most likely) used with hadoop.
 *
 * See:  https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html
 *
 * Create your key stores manually with the {@code keytool} command, or use hadoop like so:
 *
 * {@code $ hadoop credential create ssl.server.keystore.password -provider jceks://file/tmp/test.jceks}
 *
 * Unlike other Sensitive Property Providers, this provider performs retrieval only, no encryption.
 */
public class HadoopCredentialsSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(HadoopCredentialsSensitivePropertyProvider.class);

    private static final String PROVIDER_NAME = "Hadoop Credentials Sensitive Property Provider";
    private static final String PROVIDER_PREFIX = "hadoop";

    private static final String MATERIAL_SEPARATOR = "/";
    static final String PATH_SEPARATOR = ",";

    // The system property that hadoop uses to reference the key store:
    static final String KEYSTORE_PATHS_SYS_PROP = "hadoop.security.credential.provider.path";

    // The hard-coded password used by hadoop with "hadoop credentials" cli:
    static final String KEYSTORE_PASSWORD_DEFAULT = "none";

    // The environment variable hadoop uses to reference the key store password:
    private static final String KEYSTORE_PASSWORD_ENV_VAR = "HADOOP_CREDSTORE_PASSWORD";

    // The system property that hadoop uses to reference a file containing the key store password:
    static final String KEYSTORE_PASSWORD_FILE_SYS_PROP = "hadoop.security.credstore.java-keystore-provider.password-file";

    private final String[] paths;
    private KeyStore keyStore = null;
    private String keyStorePassword = null;

    /**
     * Constructs a provider using the defaults.
     */
    public HadoopCredentialsSensitivePropertyProvider() {
        this(null);
    }

    /**
     * Constructs a provider with explicit paths.
     *
     * @param paths comma-separated list of key store paths; if null, system property {@link #KEYSTORE_PATHS_SYS_PROP} will be used
     */
    public HadoopCredentialsSensitivePropertyProvider(String paths)  {
        this.paths = getKeyStorePaths(paths);
        logger.debug("{} using paths {}", getName(), paths);
    }

    private String[] getKeyStorePasswords() {
        String fromEnv = System.getenv(KEYSTORE_PASSWORD_ENV_VAR);
        if (StringUtils.isBlank(fromEnv))
            fromEnv = "";

        String fromFile = "";
        String passwordFile = System.getProperty(KEYSTORE_PASSWORD_FILE_SYS_PROP);
        if (StringUtils.isNotBlank(passwordFile)) {
            try {
                fromFile = new String(Files.readAllBytes(new File(passwordFile).toPath()), Charset.defaultCharset());
            } catch (IOException e) {
                throw new SensitivePropertyConfigurationException(e);
            }
        }
        return new String[]{KEYSTORE_PASSWORD_DEFAULT, fromEnv, fromFile};
    }


    private String[] getKeyStorePaths(String raw) {
        if (StringUtils.isBlank(raw))
            raw = System.getProperty(KEYSTORE_PATHS_SYS_PROP);

        if (StringUtils.isBlank(raw))
            throw new SensitivePropertyConfigurationException("No key store path(s) specified.");

        String prefix = PROVIDER_PREFIX + MATERIAL_SEPARATOR;
        if (raw.startsWith(prefix))
            raw = raw.substring(prefix.length());

        return raw.split(PATH_SEPARATOR);
    }

    public static String formatForType(String paths) {
        return PROVIDER_PREFIX + MATERIAL_SEPARATOR + paths;
    }

    /**
     * Returns the name of the underlying implementation.
     *
     * @return the name of this sensitive property provider
     */
    @Override
    public String getName() {
        return PROVIDER_NAME;
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return PROVIDER_PREFIX + MATERIAL_SEPARATOR + StringUtils.join(paths, PATH_SEPARATOR);
    }

    /**
     * This implementation always throws a SensitivePropertyProtectionException because we don't support
     * putting values into key stores, only extracting them.
     *
     * @param unprotectedValue the sensitive value
     * @return the value to persist in the {@code nifi.properties} file
     */
    @Override
    public String protect(String unprotectedValue) throws SensitivePropertyProtectionException {
        throw new SensitivePropertyProtectionException(getName() + " cannot protect values");
    }

    /**
     * Returns the "unprotected" form of this value.
     *
     * This implementation returns the string value of the key (as per the "hadoop credential" command).
     *
     * @param protectedValue the protected value read from the {@code nifi.properties} file
     * @return the raw value to be used by the application
     */
    @Override
    public String unprotect(String protectedValue) throws SensitivePropertyProtectionException {
        loadKeyStore();

        try {
            Key key = keyStore.getKey(protectedValue, keyStorePassword.toCharArray());
            if (key == null)
                throw new SensitivePropertyProtectionException("Value not found at key.");
            return new String(key.getEncoded(), Charset.defaultCharset());
        } catch (NoSuchAlgorithmException | UnrecoverableEntryException | KeyStoreException e) {
            throw new SensitivePropertyProtectionException(e);
        }
    }

    /**
     * This method returns the first loadable key store instance referenced by the paths array.
     *
     * @throws SensitivePropertyProtectionException when no key store is loadable
     */
    private void loadKeyStore() {
        for (String filename : paths) {
            for (String password : getKeyStorePasswords()) {
                try {
                    keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    keyStore.load(new FileInputStream(new File(filename)), password.toCharArray());
                    keyStorePassword = password;
                    return;
                } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException | IllegalArgumentException e) {
                    // continue to the next, if any
                }
            }
        }
        throw new SensitivePropertyProtectionException("No key store found or keystore password incorrect.");
    }

    /**
     * True when the client specifies a key like 'hadoop/...'.
     *
     * @param material name of encryption or protection scheme
     * @return true if this class can provide protected values
     */
    public static boolean isProviderFor(String material) {
        if (StringUtils.isBlank(material)) {
            return false;
        }
        String prefix = PROVIDER_PREFIX + MATERIAL_SEPARATOR;
        return material.startsWith(prefix) && (material.length() > prefix.length());
    }

    /**
     * Returns a printable representation of a key.
     *
     * @param key key material or key id
     * @return printable string
     */
    public static String toPrintableString(String key) {
        return key;
    }
}
