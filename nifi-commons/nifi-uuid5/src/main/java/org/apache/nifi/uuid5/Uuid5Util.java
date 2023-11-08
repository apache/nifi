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

package org.apache.nifi.uuid5;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/**
 * UUID Version 5 generator based on RFC 4122 Section 4.3 with SHA-1 hash algorithm for names
 */
public class Uuid5Util {
    private static final String NAME_DIGEST_ALGORITHM = "SHA-1";

    private static final int NAMESPACE_LENGTH = 16;

    private static final int ENCODED_LENGTH = 32;

    private static final char SEPARATOR = '-';

    /**
     * Generate UUID Version 5 based on SHA-1 hash of name with optional UUID namespace
     *
     * @param name Name for deterministic UUID generation with SHA-1 hash
     * @param namespace Optional namespace defaults to 0 when not provided
     * @return UUID string consisting of 36 lowercase characters
     */
    public static String fromString(final String name, final String namespace) {
        Objects.requireNonNull(name, "Name required");
        final UUID namespaceId = namespace == null ? new UUID(0, 0) : UUID.fromString(namespace);

        final byte[] subject = name.getBytes();
        final int nameLength = NAMESPACE_LENGTH + subject.length;
        final ByteBuffer nameBuffer = ByteBuffer.allocate(nameLength);
        nameBuffer.putLong(namespaceId.getMostSignificantBits());
        nameBuffer.putLong(namespaceId.getLeastSignificantBits());
        nameBuffer.put(subject);

        final byte[] nameHash = getNameHash(nameBuffer.array());
        final byte[] nameUuid = Arrays.copyOf(nameHash, NAMESPACE_LENGTH);

        nameUuid[6] &= 0x0F;
        nameUuid[6] |= 0x50;
        nameUuid[8] &= 0x3f;
        nameUuid[8] |= (byte) 0x80;

        final String encoded = HexFormat.of().formatHex(nameUuid);
        final StringBuilder builder = new StringBuilder(encoded);

        while (builder.length() != ENCODED_LENGTH) {
            builder.insert(0, "0");
        }

        builder.ensureCapacity(ENCODED_LENGTH);
        builder.insert(8, SEPARATOR);
        builder.insert(13, SEPARATOR);
        builder.insert(18, SEPARATOR);
        builder.insert(23, SEPARATOR);

        return builder.toString();
    }

    private static byte[] getNameHash(final byte[] name) {
        try {
            final MessageDigest messageDigest = MessageDigest.getInstance(NAME_DIGEST_ALGORITHM);
            return messageDigest.digest(name);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("UUID Name Digest Algorithm not found", e);
        }
    }
}
