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
package org.apache.nifi.security.proxied.entity;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Standard implementation singleton instance of Proxied Entity Encoder
 */
public class StandardProxiedEntityEncoder implements ProxiedEntityEncoder {
    private static final String DELIMITED_FORMAT = "<%s>";

    private static final String GT = ">";

    private static final String ESCAPED_GT = "\\\\>";

    private static final String LT = "<";

    private static final String ESCAPED_LT = "\\\\<";

    private static final Charset headerValueCharset = StandardCharsets.US_ASCII;

    private static final Base64.Encoder headerValueEncoder = Base64.getEncoder();

    private static final StandardProxiedEntityEncoder encoder = new StandardProxiedEntityEncoder();

    private StandardProxiedEntityEncoder() {

    }

    /**
     * Get singleton instance of standard encoder
     *
     * @return Proxied Entity Encoder
     */
    public static ProxiedEntityEncoder getInstance() {
        return encoder;
    }

    /**
     * Get encoded entity sanitized and formatted for transmission as an HTTP header
     *
     * @param identity Identity to be encoded
     * @return Encoded Entity
     */
    @Override
    public String getEncodedEntity(final String identity) {
        final String sanitizedIdentity = getSanitizedIdentity(identity);
        return DELIMITED_FORMAT.formatted(sanitizedIdentity);
    }

    private String getSanitizedIdentity(final String identity) {
        final String sanitized;

        if (identity == null || identity.isEmpty()) {
            sanitized = identity;
        } else {
            final String escaped = identity.replaceAll(LT, ESCAPED_LT).replaceAll(GT, ESCAPED_GT);

            // Create method-local CharsetEncoder for thread-safe state handling
            final CharsetEncoder charsetEncoder = headerValueCharset.newEncoder();
            if (charsetEncoder.canEncode(escaped)) {
                // Strings limited to US-ASCII characters can be transmitted as HTTP header values without encoding
                sanitized = escaped;
            } else {
                // Non-ASCII characters require Base64 encoding and additional wrapping for transmission as headers
                final byte[] escapedBinary = escaped.getBytes(StandardCharsets.UTF_8);
                final String escapedEncoded = headerValueEncoder.encodeToString(escapedBinary);
                sanitized = DELIMITED_FORMAT.formatted(escapedEncoded);
            }
        }

        return sanitized;
    }
}
