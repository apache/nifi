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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.PutAzureBlobStorage;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestAzureBlobClientSideEncryptionUtils {
    private static final String KEY_ID_VALUE = "key:id";
    private static final String KEY_64B_VALUE = "1234567890ABCDEF";
    private static final String KEY_128B_VALUE = KEY_64B_VALUE + KEY_64B_VALUE;
    private static final String KEY_192B_VALUE = KEY_128B_VALUE + KEY_64B_VALUE;
    private static final String KEY_256B_VALUE = KEY_128B_VALUE + KEY_128B_VALUE;
    private static final String KEY_384B_VALUE = KEY_256B_VALUE + KEY_128B_VALUE;
    private static final String KEY_512B_VALUE = KEY_256B_VALUE + KEY_256B_VALUE;

    private MockProcessContext processContext;
    private MockValidationContext validationContext;

    @Before
    public void setUp() {
        Processor processor = new PutAzureBlobStorage();
        processContext = new MockProcessContext(processor);
        validationContext = new MockValidationContext(processContext);
    }

    @Test
    public void testNoCesConfiguredOnProcessor() {
        configureProcessorProperties("NONE", null,null);

        Collection<ValidationResult> result = AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testSymmetricCesNoKeyIdOnProcessor() {
        configureProcessorProperties("SYMMETRIC", null, KEY_128B_VALUE);

        Collection<ValidationResult> result = AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testSymmetricCesNoKeyOnProcessor() {
        configureProcessorProperties("SYMMETRIC", KEY_ID_VALUE,null);

        Collection<ValidationResult> result = AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testSymmetricCesInvalidHexKeyOnProcessor() {
        configureProcessorProperties("SYMMETRIC", KEY_ID_VALUE,"ZZ");

        Collection<ValidationResult> result = AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testSymmetricCes64BitKeyOnProcessor() {
        configureProcessorProperties("SYMMETRIC", KEY_ID_VALUE, KEY_64B_VALUE);

        Collection<ValidationResult> result = AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testSymmetricCes128BitKeyOnProcessor() {
        configureProcessorProperties("SYMMETRIC", KEY_ID_VALUE, KEY_128B_VALUE);

        Collection<ValidationResult> result = AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testSymmetricCes192BitKeyOnProcessor() {
        configureProcessorProperties("SYMMETRIC", KEY_ID_VALUE, KEY_192B_VALUE);

        Collection<ValidationResult> result = AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testSymmetricCes256BitKeyOnProcessor() {
        configureProcessorProperties("SYMMETRIC", KEY_ID_VALUE, KEY_256B_VALUE);

        Collection<ValidationResult> result = AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testSymmetricCes384BitKeyOnProcessor() {
        configureProcessorProperties("SYMMETRIC", KEY_ID_VALUE, KEY_384B_VALUE);

        Collection<ValidationResult> result = AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testSymmetricCes512BitKeyOnProcessor() {
        configureProcessorProperties("SYMMETRIC", KEY_ID_VALUE, KEY_512B_VALUE);

        Collection<ValidationResult> result = AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext);

        assertValid(result);
    }

    private void configureProcessorProperties(String keyType, String keyId, String symmetricKeyHex) {
        if (keyType != null) {
            processContext.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE, keyType);
        }
        if (keyId != null) {
            processContext.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID, keyId);
        }
        if (symmetricKeyHex != null) {
            processContext.setProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX, symmetricKeyHex);
        }
    }

    private void assertValid(Collection<ValidationResult> result) {
        assertTrue("There should be no validation error", result.isEmpty());
    }

    private void assertNotValid(Collection<ValidationResult> result) {
        assertFalse("There should be validation error", result.isEmpty());
    }

}
