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
package org.apache.nifi.processors.azure.storage.utils;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class AzureBlobClientSideEncryptionUtils_v12 {

    private static final String DEFAULT_KEY_ID = "nifi";
    public static final int KeySize128 = 128 >> 3;
    public static final int KeySize192 = 192 >> 3;
    public static final int KeySize256 = 256 >> 3;
    public static final int KeySize384 = 384 >> 3;
    public static final int KeySize512 = 512 >> 3;

    public static final PropertyDescriptor CSE_KEY_TYPE = new PropertyDescriptor.Builder()
            .name("cse-key-type")
            .displayName("Client-Side Encryption Key Type")
            .required(true)
            .allowableValues(buildCseEncryptionMethodAllowableValues())
            .defaultValue(AzureBlobClientSideEncryptionMethod_v12.NONE.name())
            .description("Specifies the key type to use for client-side encryption.")
            .build();

    public static final PropertyDescriptor CSE_KEY_ID = new PropertyDescriptor.Builder()
            .name("cse-key-id")
            .displayName("Client-Side Encryption Key ID")
            .description("Specifies the ID of the key to use for client-side encryption.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(CSE_KEY_TYPE, AzureBlobClientSideEncryptionMethod_v12.LOCAL.name())
            .build();

    public static final PropertyDescriptor CSE_LOCAL_KEY_HEX = new PropertyDescriptor.Builder()
            .name("cse-local-key-hex")
            .displayName("Local Key")
            .description("When using local client-side encryption, this is the raw key, encoded in hexadecimal")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(CSE_KEY_TYPE, AzureBlobClientSideEncryptionMethod_v12.LOCAL.name())
            .sensitive(true)
            .build();

    private static AllowableValue[] buildCseEncryptionMethodAllowableValues() {
        return Arrays.stream(AzureBlobClientSideEncryptionMethod_v12.values())
            .map(v -> new AllowableValue(v.name(), v.name(), v.getDescription()))
            .toArray(AllowableValue[]::new);
    }

    public static Collection<ValidationResult> validateClientSideEncryptionProperties(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>();

        final String cseKeyTypeValue = validationContext.getProperty(CSE_KEY_TYPE).getValue();
        final AzureBlobClientSideEncryptionMethod_v12 cseKeyType = AzureBlobClientSideEncryptionMethod_v12.valueOf(cseKeyTypeValue);

        final String cseKeyId = validationContext.getProperty(CSE_KEY_ID).getValue();

        final String cseLocalKeyHex = validationContext.getProperty(CSE_LOCAL_KEY_HEX).getValue();

        if (cseKeyType != AzureBlobClientSideEncryptionMethod_v12.NONE && StringUtils.isBlank(cseKeyId)) {
            validationResults.add(new ValidationResult.Builder().subject(CSE_KEY_ID.getDisplayName())
                    .explanation("a key ID must be set when client-side encryption is enabled.").build());
        }

        if (cseKeyType == AzureBlobClientSideEncryptionMethod_v12.LOCAL) {
            validationResults.addAll(validateLocalKey(cseLocalKeyHex));
        }

        return validationResults;
    }

    private static List<ValidationResult> validateLocalKey(String keyHex) {
        final List<ValidationResult> validationResults = new ArrayList<>();
        if (StringUtils.isBlank(keyHex)) {
            validationResults.add(new ValidationResult.Builder().subject(CSE_LOCAL_KEY_HEX.getDisplayName())
                    .explanation("a local key must be set when client-side encryption is enabled with local encryption.").build());
        } else {
            byte[] keyBytes;
            try {
                keyBytes = Hex.decodeHex(keyHex.toCharArray());
                if (keyBytes.length != KeySize128 && keyBytes.length != KeySize192 && keyBytes.length != KeySize256 && keyBytes.length != KeySize384 && keyBytes.length != KeySize512) {
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

}
