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
package org.apache.nifi.properties.sensitive;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.aes.AESSensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.aws.kms.AWSKMSSensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.azure.keyvault.AzureKeyVaultSensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.hadoop.HadoopCredentialsSensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.hashicorp.vault.VaultSensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.keystore.KeyStoreWrappedSensitivePropertyProvider;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Security;

/**
 *
 * This class describes the available sensitive property providers {@link SensitivePropertyProvider} which can be chosen to encrypt/decrypt the sensitive properties in the nifi.properties file.
 * Most of these providers will require external setup in their respective system (eg. Hashicorp Vault) and then configuration given in the nifi.properties file.
 *
 * For further details, review the <a href="https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#sensitive_property_providers">Apache NiFi User Guide - Sensitive Property Providers</a> section.
 *
 * As of Apache NiFi 1.12.0 this implementation is considered <a href="https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#experimental_warning">*experimental*</a>.
 * Available implementations can be found listed in StandardSensitivePropertyProvider.
 *
 * This class hides the various SPP subclass construction from clients.
 *
 */
public class StandardSensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(StandardSensitivePropertyProvider.class);

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    /**
     * Creates a {@link SensitivePropertyProvider} suitable for a given key or key id.
     *
     * If an empty or null key/key id is given, this implementation returns null.  This is a convenience
     * for clients using the various Property classes, as those classes allow a null SensitivePropertyProvider.
     *
     * If no provider recognizes a key/key id, this implementation throws {@link SensitivePropertyProtectionException}.
     *
     * @param key provider encryption key
     * @return concrete instance of SensitivePropertyProvider, or null when no key/key id is specified
     * @throws SensitivePropertyProtectionException when a key/key id is not handled by any provider.
     */
    public static SensitivePropertyProvider fromKey(String key) {
        if (StringUtils.isBlank(key)) {
            return null;

        } else if (HadoopCredentialsSensitivePropertyProvider.isProviderFor(key)) {
            logger.debug("StandardSensitivePropertyProvider selected specific Hadoop Credential provider for key: " + HadoopCredentialsSensitivePropertyProvider.toPrintableString(key));
            return new HadoopCredentialsSensitivePropertyProvider(key);

        } else if (AzureKeyVaultSensitivePropertyProvider.isProviderFor(key)) {
            logger.debug("StandardSensitivePropertyProvider selected specific Azure Key Vault provider for key: " + AzureKeyVaultSensitivePropertyProvider.toPrintableString(key));
            return new AzureKeyVaultSensitivePropertyProvider(key);

        } else if (VaultSensitivePropertyProvider.isProviderFor(key)) {
            logger.debug("StandardSensitivePropertyProvider selected specific HashiCorp Vault provider for key: " + VaultSensitivePropertyProvider.toPrintableString(key));
            return new VaultSensitivePropertyProvider(key);

        } else if (KeyStoreWrappedSensitivePropertyProvider.isProviderFor(key)) {
            logger.debug("StandardSensitivePropertyProvider selected specific KeyStore provider for key: " + KeyStoreWrappedSensitivePropertyProvider.toPrintableString(key));
            return new KeyStoreWrappedSensitivePropertyProvider(key);

        } else if (AWSKMSSensitivePropertyProvider.isProviderFor(key)) {
            logger.debug("StandardSensitivePropertyProvider selected specific AWS KMS provider for key: " + AWSKMSSensitivePropertyProvider.toPrintableString(key));
            return new AWSKMSSensitivePropertyProvider(key);

        } else if (AESSensitivePropertyProvider.isProviderFor(key)) {
            logger.debug("StandardSensitivePropertyProvider selected specific AES provider for key: " + AESSensitivePropertyProvider.toPrintableString(key));
            return new AESSensitivePropertyProvider(key);
        }

        throw new SensitivePropertyProtectionException("No sensitive property provider for key or key id.");
    }

    /**
     * True if at least one known sensitive property provider implements protect/unprotect for the given scheme.
     *
     * @param scheme name of encryption or protection scheme
     * @return true if at least one provider handles scheme
     */
    public static boolean hasProviderFor(String scheme) {
        return HadoopCredentialsSensitivePropertyProvider.isProviderFor(scheme)
                || AzureKeyVaultSensitivePropertyProvider.isProviderFor(scheme)
                || VaultSensitivePropertyProvider.isProviderFor(scheme)
                || KeyStoreWrappedSensitivePropertyProvider.isProviderFor(scheme)
                || AWSKMSSensitivePropertyProvider.isProviderFor(scheme)
                || AESSensitivePropertyProvider.isProviderFor(scheme);
    }

    /**
     * @return the default protection scheme from the default provider.
     */
    public static String getDefaultProtectionScheme() {
        return AESSensitivePropertyProvider.getDefaultProtectionScheme();
    }
}
