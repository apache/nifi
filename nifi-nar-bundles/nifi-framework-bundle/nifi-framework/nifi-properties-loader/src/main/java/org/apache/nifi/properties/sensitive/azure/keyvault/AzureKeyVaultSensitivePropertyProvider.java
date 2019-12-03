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
package org.apache.nifi.properties.sensitive.azure.keyvault;

import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.models.KeyOperationResult;
import com.microsoft.azure.keyvault.models.KeyVaultErrorException;
import com.microsoft.azure.keyvault.webkey.JsonWebKeyEncryptionAlgorithm;
import com.microsoft.azure.management.Azure;
import com.microsoft.rest.LogLevel;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.SensitivePropertyConfigurationException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.DecoderException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


/**
 * This provider uses the Azure SDK to interact with the Azure Key Vault service.
 *
 * Azure with Java:
 * https://docs.microsoft.com/en-us/azure/java/?view=azure-java-stable
 *
 * Azure auth with Java:
 * https://github.com/Azure/azure-libraries-for-java/blob/master/AUTH.md
 *
 * Azure Key Vault with Java:
 * https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.keyvault?view=azure-java-stable
 */
public class AzureKeyVaultSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final String IMPLEMENTATION_NAME = "Azure Key Vault Sensitive Property Provider";
    private static final String MATERIAL_PROVIDER = "azure";
    private static final String MATERIAL_KEY_TYPE = "vault";
    private static final String IMPLEMENTATION_PREFIX = MATERIAL_PROVIDER + "/" + MATERIAL_KEY_TYPE;
    private static final int MAX_PROTECT_LENGTH = 470;

    private static final JsonWebKeyEncryptionAlgorithm algo = JsonWebKeyEncryptionAlgorithm.RSA_OAEP;
    private static final Map<Integer, Integer> maxValueSizes = new HashMap<>();
    static {
        maxValueSizes.put(1024, (1024/8) - 42); // 86   // rsa 1024 + oaep
        maxValueSizes.put(2048, (2048/8) - 42); // 214  // rsa 2048 + oaep
        // rsa 1_5?
    }

    private final KeyVaultClient client;
    private final String vaultId;
    private final String keyId;


    public AzureKeyVaultSensitivePropertyProvider(String material) {
        final File credFile = new File(System.getenv("AZURE_AUTH_LOCATION"));
        try {
            ApplicationTokenCredentials.fromFile(credFile).clientId();
        } catch (IOException e) {
            throw new SensitivePropertyConfigurationException(e);
        }

        String prefix = IMPLEMENTATION_PREFIX + "/";
        if (material.startsWith(prefix)) {
            material = material.substring(prefix.length());
        }

        String[] parts = material.split(",");
        this.vaultId = parts[0];
        this.keyId = parts[1];

        try {
            Azure azure = Azure.configure()
                    .withLogLevel(LogLevel.BASIC)
                    .authenticate(credFile)
                    .withDefaultSubscription();
            this.client = azure.vaults().getById(this.vaultId).client();
        } catch (IOException e) {
            throw new SensitivePropertyConfigurationException(e);
        }
    }

    public static boolean isProviderFor(String key) {
        return key.startsWith(IMPLEMENTATION_PREFIX + "/");
    }

    public static String toPrintableString(String key) {
        return key;
    }

    /**
     * Returns the name of the underlying implementation.
     *
     * @return the name of this sensitive property provider
     */
    @Override
    public String getName() {
        return IMPLEMENTATION_NAME;
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return IMPLEMENTATION_PREFIX + vaultId + "," + keyId;
    }

    /**
     * Returns the "protected" form of this value. This is a form which can safely be persisted in the {@code nifi.properties} file without compromising the value.
     * An encryption-based provider would return a cipher text, while a remote-lookup provider could return a unique ID to retrieve the secured value.
     *
     * @param unprotectedValue the sensitive value
     * @return the value to persist in the {@code nifi.properties} file
     */
    @Override
    public String protect(String unprotectedValue) throws SensitivePropertyProtectionException {
        // TODO:  check length of value; azure limits length of encryption operations.

        if (StringUtils.isBlank(unprotectedValue)) {
            throw new IllegalArgumentException("Cannot encrypt an empty value");
        } else if (unprotectedValue.length() > MAX_PROTECT_LENGTH) {
            throw new IllegalArgumentException("Azure SPP cannot encrypt value longer than 128 characters, have " + unprotectedValue.length());
        }

        KeyOperationResult operation;
        try {
            operation = client.encrypt(keyId, algo, unprotectedValue.getBytes(StandardCharsets.UTF_8));
        } catch (final KeyVaultErrorException e) {
            throw new SensitivePropertyProtectionException(e);
        }
        byte[] cipherText = operation.result();
        return Base64.toBase64String(cipherText);
    }

    /**
     * Returns the "unprotected" form of this value. This is the raw sensitive value which is used by the application logic.
     * An encryption-based provider would decrypt a cipher text and return the plaintext, while a remote-lookup provider could retrieve the secured value.
     *
     * @param protectedValue the protected value read from the {@code nifi.properties} file
     * @return the raw value to be used by the application
     */
    @Override
    public String unprotect(String protectedValue) throws SensitivePropertyProtectionException {
        byte[] decoded;
        try {
            decoded = Base64.decode(protectedValue);
        } catch (final DecoderException e) {
            throw new SensitivePropertyProtectionException(e);
        }

        KeyOperationResult operation;
        try {
            operation = client.decrypt(keyId, algo, decoded);
        } catch (final KeyVaultErrorException e) {
            throw new SensitivePropertyProtectionException(e);
        }
        return new String(operation.result(), StandardCharsets.UTF_8);
    }
}
