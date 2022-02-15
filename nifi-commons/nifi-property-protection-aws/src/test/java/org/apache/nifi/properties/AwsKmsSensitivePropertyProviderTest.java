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
package org.apache.nifi.properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
public class AwsKmsSensitivePropertyProviderTest {
    private static final String PROPERTY_NAME = String.class.getSimpleName();

    private static final String PROPERTY = String.class.getName();

    private static final String ENCRYPTED_PROPERTY = Integer.class.getName();

    private static final byte[] ENCRYPTED_BYTES = ENCRYPTED_PROPERTY.getBytes(StandardCharsets.UTF_8);

    private static final String PROTECTED_PROPERTY = Base64.getEncoder().withoutPadding().encodeToString(ENCRYPTED_BYTES);

    private static final String KEY_ID = AwsKmsSensitivePropertyProvider.class.getSimpleName();

    private static final Properties PROPERTIES = new Properties();

    private static final String IDENTIFIER_KEY = "aws/kms";

    static {
        PROPERTIES.setProperty(AwsKmsSensitivePropertyProvider.KEY_ID_PROPERTY, KEY_ID);
    }

    @Mock
    private KmsClient kmsClient;

    private AwsKmsSensitivePropertyProvider provider;

    @BeforeEach
    public void setProvider() {
        final KeyMetadata keyMetadata = KeyMetadata.builder().enabled(true).build();
        final DescribeKeyResponse describeKeyResponse = DescribeKeyResponse.builder().keyMetadata(keyMetadata).build();
        when(kmsClient.describeKey(any(DescribeKeyRequest.class))).thenReturn(describeKeyResponse);

        provider = new AwsKmsSensitivePropertyProvider(kmsClient, PROPERTIES);
    }

    @Test
    public void testValidateClientNull() {
        final AwsKmsSensitivePropertyProvider provider = new AwsKmsSensitivePropertyProvider(null, PROPERTIES);
        assertNotNull(provider);
    }

    @Test
    public void testValidateKeyDisabled() {
        final KeyMetadata keyMetadata = KeyMetadata.builder().enabled(false).build();
        final DescribeKeyResponse describeKeyResponse = DescribeKeyResponse.builder().keyMetadata(keyMetadata).build();
        when(kmsClient.describeKey(any(DescribeKeyRequest.class))).thenReturn(describeKeyResponse);

        assertThrows(SensitivePropertyProtectionException.class, () -> new AwsKmsSensitivePropertyProvider(kmsClient, PROPERTIES));
    }

    @Test
    public void testCleanUp() {
        provider.cleanUp();
        verify(kmsClient).close();
    }

    @Test
    public void testProtect() {
        final SdkBytes blob = SdkBytes.fromUtf8String(ENCRYPTED_PROPERTY);
        final EncryptResponse encryptResponse = EncryptResponse.builder().ciphertextBlob(blob).build();
        when(kmsClient.encrypt(any(EncryptRequest.class))).thenReturn(encryptResponse);

        final String protectedProperty = provider.protect(PROPERTY, ProtectedPropertyContext.defaultContext(PROPERTY_NAME));
        assertEquals(PROTECTED_PROPERTY, protectedProperty);
    }

    @Test
    public void testUnprotect() {
        final SdkBytes blob = SdkBytes.fromUtf8String(PROPERTY);
        final DecryptResponse decryptResponse = DecryptResponse.builder().plaintext(blob).build();
        when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(decryptResponse);

        final String property = provider.unprotect(PROTECTED_PROPERTY, ProtectedPropertyContext.defaultContext(PROPERTY_NAME));
        assertEquals(PROPERTY, property);
    }

    @Test
    public void testGetIdentifierKey() {
        final String identifierKey = provider.getIdentifierKey();
        assertEquals(IDENTIFIER_KEY, identifierKey);
    }
}
