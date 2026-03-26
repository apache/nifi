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
package org.apache.nifi.services.iceberg.gcs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Computes a Base64-encoded SHA-256 digest of a Base64-encoded encryption objectKey,
 * as required by the GCS JSON API customer-supplied encryption objectKey headers.
 */
class KeyDigestProvider {

    private static final String DIGEST_ALGORITHM = "SHA-256";

    private KeyDigestProvider() {
    }

    static String getDigestEncoded(final String keyEncoded) {
        try {
            final byte[] keyDecoded = Base64.getDecoder().decode(keyEncoded);
            final byte[] hash = MessageDigest.getInstance(DIGEST_ALGORITHM).digest(keyDecoded);
            return Base64.getEncoder().encodeToString(hash);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not found", e);
        }
    }
}
