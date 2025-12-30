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

import org.apache.commons.codec.binary.Base64;
import software.amazon.encryption.s3.CommitmentPolicy;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

final class S3EncryptionTestUtil {

    private static final Random RANDOM = new Random();

    private S3EncryptionTestUtil() {
    }

    static String createCustomerKey(int keySize) {
        return base64Encode(createRawKey(keySize));
    }

    static S3EncryptionKeySpec createCustomerKeySpec(int keySize) {
        return createCustomerKeySpec(keySize, null);
    }

    static S3EncryptionKeySpec createCustomerKeySpec(int keySize, CommitmentPolicy commitmentPolicy) {
        byte[] keyMaterial = createRawKey(keySize);
        byte[] keyMaterialMd5 = md5(keyMaterial);

        return new S3EncryptionKeySpec(null, base64Encode(keyMaterial), base64Encode(keyMaterialMd5), commitmentPolicy);
    }

    private static byte[] createRawKey(int keySize) {
        if (keySize % 8 != 0) {
            throw new IllegalArgumentException("Invalid test data");
        }

        byte[] keyMaterial = new byte[keySize / 8];
        RANDOM.nextBytes(keyMaterial);
        return keyMaterial;
    }

    private static String base64Encode(byte[] payload) {
        return Base64.encodeBase64String(payload);
    }

    private static byte[] md5(byte[] payload) {
        try {
            return MessageDigest.getInstance("MD5").digest(payload);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
