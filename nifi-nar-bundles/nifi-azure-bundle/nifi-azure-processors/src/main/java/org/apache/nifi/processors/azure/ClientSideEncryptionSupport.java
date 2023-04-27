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

public interface ClientSideEncryptionSupport {
    List<KeyOperation> KEY_OPERATIONS = Arrays.asList(KeyOperation.WRAP_KEY, KeyOperation.UNWRAP_KEY);

    PropertyDescriptor CSE_KEY_TYPE = new PropertyDescriptor.Builder()
            .name("cse-key-type")
            .displayName("Client-Side Encryption Key Type")
            .required(true)
            .allowableValues(ClientSideEncryptionMethod.class)
            .defaultValue(ClientSideEncryptionMethod.NONE.name())
            .description("Specifies the key type to use for client-side encryption.")
            .build();

    PropertyDescriptor CSE_KEY_ID = new PropertyDescriptor.Builder()
            .name("cse-key-id")
            .displayName("Client-Side Encryption Key ID")
            .description("Specifies the ID of the key to use for client-side encryption.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(CSE_KEY_TYPE, ClientSideEncryptionMethod.LOCAL.name())
            .build();

    PropertyDescriptor CSE_LOCAL_KEY_HEX = new PropertyDescriptor.Builder()
            .name("cse-local-key-hex")
            .displayName("Client-Side Encryption Local Key")
            .description("When using local client-side encryption, this is the raw key, encoded in hexadecimal")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(CSE_KEY_TYPE, ClientSideEncryptionMethod.LOCAL.name())
            .sensitive(true)
            .build();

    default Collection<ValidationResult> validateClientSideEncryptionProperties(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>();
        final String cseKeyTypeValue = validationContext.getProperty(CSE_KEY_TYPE).getValue();
        final ClientSideEncryptionMethod cseKeyType = ClientSideEncryptionMethod.valueOf(cseKeyTypeValue);
        final String cseKeyId = validationContext.getProperty(CSE_KEY_ID).getValue();
        final String cseLocalKeyHex = validationContext.getProperty(CSE_LOCAL_KEY_HEX).getValue();
        if (cseKeyType != ClientSideEncryptionMethod.NONE && StringUtils.isBlank(cseKeyId)) {
            validationResults.add(new ValidationResult.Builder().subject(CSE_KEY_ID.getDisplayName())
                    .explanation("a key ID must be set when client-side encryption is enabled.").build());
        }
        if (cseKeyType == ClientSideEncryptionMethod.LOCAL) {
            validationResults.addAll(validateLocalKey(cseLocalKeyHex));
        }
        return validationResults;
    }

    default List<ValidationResult> validateLocalKey(String keyHex) {
        final List<ValidationResult> validationResults = new ArrayList<>();
        if (StringUtils.isBlank(keyHex)) {
            validationResults.add(new ValidationResult.Builder().subject(CSE_LOCAL_KEY_HEX.getDisplayName())
                    .explanation("a local key must be set when client-side encryption is enabled with local encryption.").build());
        } else {
            byte[] keyBytes;
            try {
                keyBytes = Hex.decodeHex(keyHex);
                if (getKeyWrapAlgorithm(keyBytes) == null) {
                    validationResults.add(new ValidationResult.Builder().subject(CSE_LOCAL_KEY_HEX.getDisplayName())
                            .explanation("the local key must be 128, 192, 256, 384 or 512 bits of data.").build());
                }
            } catch (DecoderException e) {
                validationResults.add(new ValidationResult.Builder().subject(CSE_LOCAL_KEY_HEX.getDisplayName())
                        .explanation("the local key must be a valid hexadecimal string.").build());
            } catch (IllegalArgumentException e) {
                validationResults.add(new ValidationResult.Builder().subject(CSE_LOCAL_KEY_HEX.getDisplayName())
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
        final String cseLocalKeyHex = context.getProperty(CSE_LOCAL_KEY_HEX).getValue();
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        byte[] keyBytes = Hex.decodeHex(cseLocalKeyHex);
        JsonWebKey localKey = JsonWebKey.fromAes(new SecretKeySpec(keyBytes, "AES"), KEY_OPERATIONS)
                .setId(cseKeyId);
        AsyncKeyEncryptionKey akek = new KeyEncryptionKeyClientBuilder()
                .buildAsyncKeyEncryptionKey(localKey).block();
        return new EncryptedBlobClientBuilder(EncryptionVersion.V2)
                .key(akek, getKeyWrapAlgorithm(keyBytes))
                .blobClient(blobClient)
                .buildEncryptedBlobClient();
    }

    default String getKeyWrapAlgorithm(byte[] keyBytes) {
        final int KeySize128 = 128 >> 3;
        final int KeySize192 = 192 >> 3;
        final int KeySize256 = 256 >> 3;
        final int KeySize384 = 384 >> 3;
        final int KeySize512 = 512 >> 3;
        switch (keyBytes.length) {
            case KeySize128:
                return KeyWrapAlgorithm.A128KW.toString();
            case KeySize192:
                return KeyWrapAlgorithm.A192KW.toString();
            case KeySize256:
            case KeySize512:
            case KeySize384:
                // Default to longest allowed key length for wrap
                return KeyWrapAlgorithm.A256KW.toString();
            default:
                return null;
        }
    }
}
