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
package org.apache.nifi.processors.azure;

import com.azure.core.cryptography.AsyncKeyEncryptionKey;
import com.azure.security.keyvault.keys.cryptography.KeyEncryptionKeyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;
import com.azure.security.keyvault.keys.models.JsonWebKey;
import com.azure.security.keyvault.keys.models.KeyOperation;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.specialized.cryptography.EncryptedBlobClientBuilder;
import com.azure.storage.blob.specialized.cryptography.EncryptionVersion;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.ClientSideEncryptionMethod;
import org.apache.nifi.util.StringUtils;

import javax.crypto.spec.SecretKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface ClientSideEncryptionSupport {
    List<KeyOperation> KEY_OPERATIONS = Arrays.asList(KeyOperation.WRAP_KEY, KeyOperation.UNWRAP_KEY);

    PropertyDescriptor CSE_KEY_TYPE = new PropertyDescriptor.Builder()
            .name("Client-Side Encryption Key Type")
            .displayName("Client-Side Encryption Key Type")
            .required(true)
            .allowableValues(ClientSideEncryptionMethod.class)
            .defaultValue(ClientSideEncryptionMethod.NONE.getValue())
            .description("Specifies the key type to use for client-side encryption.")
            .build();

    PropertyDescriptor CSE_KEY_ID = new PropertyDescriptor.Builder()
            .name("Client-Side Encryption Key ID")
            .displayName("Client-Side Encryption Key ID")
            .description("Specifies the ID of the key to use for client-side encryption.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(CSE_KEY_TYPE, ClientSideEncryptionMethod.LOCAL)
            .build();

    PropertyDescriptor CSE_LOCAL_KEY = new PropertyDescriptor.Builder()
            .name("Client-Side Encryption Local Key")
            .displayName("Client-Side Encryption Local Key")
            .description("When using local client-side encryption, this is the raw key, encoded in hexadecimal")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(CSE_KEY_TYPE, ClientSideEncryptionMethod.LOCAL)
            .sensitive(true)
            .build();

    default Collection<ValidationResult> validateClientSideEncryptionProperties(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>();
        final String cseKeyTypeValue = validationContext.getProperty(CSE_KEY_TYPE).getValue();
        final ClientSideEncryptionMethod cseKeyType = ClientSideEncryptionMethod.valueOf(cseKeyTypeValue);
        final String cseKeyId = validationContext.getProperty(CSE_KEY_ID).getValue();
        final String cseLocalKey = validationContext.getProperty(CSE_LOCAL_KEY).getValue();
        if (cseKeyType != ClientSideEncryptionMethod.NONE && StringUtils.isBlank(cseKeyId)) {
            validationResults.add(new ValidationResult.Builder().subject(CSE_KEY_ID.getDisplayName())
                    .explanation("Key ID must be set when client-side encryption is enabled").build());
        }
        if (ClientSideEncryptionMethod.LOCAL == cseKeyType) {
            validationResults.addAll(validateLocalKey(cseLocalKey));
        }
        return validationResults;
    }

    default List<ValidationResult> validateLocalKey(String keyHex) {
        final List<ValidationResult> validationResults = new ArrayList<>();
        if (StringUtils.isBlank(keyHex)) {
            validationResults.add(new ValidationResult.Builder().subject(CSE_LOCAL_KEY.getDisplayName())
                    .explanation("Key must be set when client-side encryption is enabled").build());
        } else {
            try {
                final byte[] keyBytes = Hex.decodeHex(keyHex);
                if (getKeyWrapAlgorithm(keyBytes).isEmpty()) {
                    validationResults.add(new ValidationResult.Builder().subject(CSE_LOCAL_KEY.getDisplayName())
                            .explanation(String.format("Key size in bits must be one of [128, 192, 256, 384, 512] instead of [%d]", keyBytes.length * 8)).build());
                }
            } catch (DecoderException e) {
                validationResults.add(new ValidationResult.Builder().subject(CSE_LOCAL_KEY.getDisplayName())
                        .explanation("Key must be a valid hexadecimal string").build());
            } catch (IllegalArgumentException e) {
                validationResults.add(new ValidationResult.Builder().subject(CSE_LOCAL_KEY.getDisplayName())
                        .explanation(e.getMessage()).build());
            }
        }

        return validationResults;
    }

    default boolean isClientSideEncryptionEnabled(PropertyContext context) {
        final String cseKeyTypeValue = context.getProperty(CSE_KEY_TYPE).getValue();
        final ClientSideEncryptionMethod cseKeyType = ClientSideEncryptionMethod.valueOf(cseKeyTypeValue);
        return cseKeyType != ClientSideEncryptionMethod.NONE;
    }

    default BlobClient getEncryptedBlobClient(PropertyContext context, BlobContainerClient containerClient, String blobName) throws DecoderException {
        final String cseKeyId = context.getProperty(CSE_KEY_ID).getValue();
        final String cseLocalKeyHex = context.getProperty(CSE_LOCAL_KEY).getValue();
        final BlobClient blobClient = containerClient.getBlobClient(blobName);
        final byte[] keyBytes = Hex.decodeHex(cseLocalKeyHex);
        JsonWebKey localKey = JsonWebKey.fromAes(new SecretKeySpec(keyBytes, "AES"), KEY_OPERATIONS)
                .setId(cseKeyId);
        AsyncKeyEncryptionKey akek = new KeyEncryptionKeyClientBuilder()
                .buildAsyncKeyEncryptionKey(localKey).block();
        final String keyWrapAlgorithm = getKeyWrapAlgorithm(keyBytes).orElseThrow(() -> new IllegalArgumentException("Failed to derive key wrap algorithm"));

        return new EncryptedBlobClientBuilder(EncryptionVersion.V2)
                .key(akek, keyWrapAlgorithm)
                .blobClient(blobClient)
                .buildEncryptedBlobClient();
    }

    default Optional<String> getKeyWrapAlgorithm(byte[] keyBytes) {
        final int keySize128 = 16;
        final int keySize192 = 24;
        final int keySize256 = 32;
        final int keySize384 = 48;
        final int keySize512 = 64;
        switch (keyBytes.length) {
            case keySize128:
                return Optional.of(KeyWrapAlgorithm.A128KW.toString());
            case keySize192:
                return Optional.of(KeyWrapAlgorithm.A192KW.toString());
            case keySize256:
            case keySize384:
            case keySize512:
                // Default to longest allowed key length for wrap
                return Optional.of(KeyWrapAlgorithm.A256KW.toString());
            default:
                return Optional.empty();
        }
    }
}
