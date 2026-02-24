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
package org.apache.nifi.processors.aws.s3.encryption;

import org.apache.nifi.components.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.nifi.processors.aws.s3.encryption.S3EncryptionTestUtil.createCustomerKeySpec;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestClientSideCEncryptionStrategyKeyValidation {

    private ClientSideCEncryptionStrategy strategy;

    @BeforeEach
    public void setUp() {
        strategy = new ClientSideCEncryptionStrategy();
    }

    @Test
    public void testValid256BitKey() {
        final S3EncryptionKeySpec keySpec = createCustomerKeySpec(256);

        final ValidationResult result = strategy.validateKeySpec(keySpec);

        assertTrue(result.isValid());
    }

    @Test
    public void testValid192BitKey() {
        final S3EncryptionKeySpec keySpec = createCustomerKeySpec(192);

        final ValidationResult result = strategy.validateKeySpec(keySpec);

        assertTrue(result.isValid());
    }

    @Test
    public void testValid128BitKey() {
        final S3EncryptionKeySpec keySpec = createCustomerKeySpec(128);

        final ValidationResult result = strategy.validateKeySpec(keySpec);

        assertTrue(result.isValid());
    }

    @Test
    public void testNotSupportedKeySize() {
        final S3EncryptionKeySpec keySpec = createCustomerKeySpec(512);

        final ValidationResult result = strategy.validateKeySpec(keySpec);

        assertFalse(result.isValid());
    }

    @Test
    public void testNullKey() {
        final S3EncryptionKeySpec keySpec = new S3EncryptionKeySpec(null, null, null, null);

        final ValidationResult result = strategy.validateKeySpec(keySpec);

        assertFalse(result.isValid());
    }

    @Test
    public void testEmptyKey() {
        final S3EncryptionKeySpec keySpec = new S3EncryptionKeySpec(null, "", null, null);

        final ValidationResult result = strategy.validateKeySpec(keySpec);

        assertFalse(result.isValid());
    }

    @Test
    public void testNotBase64EncodedKey() {
        final S3EncryptionKeySpec keySpec = new S3EncryptionKeySpec(null, "NotBase64EncodedKey", null, null);

        final ValidationResult result = strategy.validateKeySpec(keySpec);

        assertFalse(result.isValid());
    }
}
