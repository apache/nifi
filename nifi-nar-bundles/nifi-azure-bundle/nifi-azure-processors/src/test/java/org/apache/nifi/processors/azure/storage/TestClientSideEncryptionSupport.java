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
package org.apache.nifi.processors.azure.storage;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processors.azure.storage.utils.ClientSideEncryptionMethod;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestClientSideEncryptionSupport {
    private static final String KEY_ID_VALUE = "key:id";
    private static final String KEY_64B_VALUE = "1234567890ABCDEF";
    private static final String KEY_128B_VALUE = KEY_64B_VALUE + KEY_64B_VALUE;
    private static final String KEY_192B_VALUE = KEY_128B_VALUE + KEY_64B_VALUE;
    private static final String KEY_256B_VALUE = KEY_128B_VALUE + KEY_128B_VALUE;
    private static final String KEY_384B_VALUE = KEY_256B_VALUE + KEY_128B_VALUE;
    private static final String KEY_512B_VALUE = KEY_256B_VALUE + KEY_256B_VALUE;

    private MockProcessContext processContext;
    private MockValidationContext validationContext;
    private PutAzureBlobStorage_v12 putAzureBlobStorage_v12;

    @BeforeEach
    public void setUp() {
        putAzureBlobStorage_v12 = new PutAzureBlobStorage_v12();
        processContext = new MockProcessContext(putAzureBlobStorage_v12);
        validationContext = new MockValidationContext(processContext);
    }

    @Test
    public void testNoCesConfiguredOnProcessor() {
        configureProcessorProperties(ClientSideEncryptionMethod.NONE, null, null);

        Collection<ValidationResult> result = putAzureBlobStorage_v12.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testLocalCesNoKeyIdOnProcessor() {
        configureProcessorProperties(ClientSideEncryptionMethod.LOCAL, null, KEY_128B_VALUE);

        Collection<ValidationResult> result = putAzureBlobStorage_v12.validateClientSideEncryptionProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testLocalCesNoKeyOnProcessor() {
        configureProcessorProperties(ClientSideEncryptionMethod.LOCAL, KEY_ID_VALUE, null);

        Collection<ValidationResult> result = putAzureBlobStorage_v12.validateClientSideEncryptionProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testLocalCesInvalidHexKeyOnProcessor() {
        configureProcessorProperties(ClientSideEncryptionMethod.LOCAL, KEY_ID_VALUE, "ZZ");

        Collection<ValidationResult> result = putAzureBlobStorage_v12.validateClientSideEncryptionProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testLocalCesInvalidKeyLengthOnProcessor() {
        configureProcessorProperties(ClientSideEncryptionMethod.LOCAL, KEY_ID_VALUE, KEY_64B_VALUE);

        Collection<ValidationResult> result = putAzureBlobStorage_v12.validateClientSideEncryptionProperties(validationContext);

        assertNotValid(result);
        assertContains(result, "Key size in bits must be one of [128, 192, 256, 384, 512] instead of [64]");
    }

    @Test
    public void testLocalCes128BitKeyOnProcessor() {
        configureProcessorProperties(ClientSideEncryptionMethod.LOCAL, KEY_ID_VALUE, KEY_128B_VALUE);

        Collection<ValidationResult> result = putAzureBlobStorage_v12.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testLocalCes192BitKeyOnProcessor() {
        configureProcessorProperties(ClientSideEncryptionMethod.LOCAL, KEY_ID_VALUE, KEY_192B_VALUE);

        Collection<ValidationResult> result = putAzureBlobStorage_v12.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testLocalCes256BitKeyOnProcessor() {
        configureProcessorProperties(ClientSideEncryptionMethod.LOCAL, KEY_ID_VALUE, KEY_256B_VALUE);

        Collection<ValidationResult> result = putAzureBlobStorage_v12.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testLocalCes384BitKeyOnProcessor() {
        configureProcessorProperties(ClientSideEncryptionMethod.LOCAL, KEY_ID_VALUE, KEY_384B_VALUE);

        Collection<ValidationResult> result = putAzureBlobStorage_v12.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testLocalCes512BitKeyOnProcessor() {
        configureProcessorProperties(ClientSideEncryptionMethod.LOCAL, KEY_ID_VALUE, KEY_512B_VALUE);

        Collection<ValidationResult> result = putAzureBlobStorage_v12.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    private void configureProcessorProperties(ClientSideEncryptionMethod keyType, String keyId, String localKeyHex) {
        if (keyType != null) {
            processContext.setProperty(putAzureBlobStorage_v12.CSE_KEY_TYPE, keyType.getValue());
        }
        if (keyId != null) {
            processContext.setProperty(putAzureBlobStorage_v12.CSE_KEY_ID, keyId);
        }
        if (localKeyHex != null) {
            processContext.setProperty(putAzureBlobStorage_v12.CSE_LOCAL_KEY, localKeyHex);
        }
    }

    private void assertValid(Collection<ValidationResult> result) {
        assertTrue(result.isEmpty(), "There should be no validation error");
    }

    private void assertNotValid(Collection<ValidationResult> result) {
        assertFalse(result.isEmpty(), "There should be validation error");
    }

    private void assertContains(Collection<ValidationResult> result, String explaination) {
        assertFalse(result.isEmpty(), "There should be validation error");
        assertTrue(result.stream().filter(v -> v.getExplanation().contains(explaination)).findFirst().isPresent());
    }
}