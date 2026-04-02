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

package org.apache.nifi.snowflake.service;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.RSAPublicKeySpec;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RSA Private Key Authorization Provider implementing Snowflake JSON Web Token with RS256
 */
class RSAKeyAuthorizationProvider {

    private static final String FINGERPRINT_ALGORITHM = "SHA-256";

    private static final String FINGERPRINT_FORMAT = "SHA256:%s";

    private static final String SIGNING_ALGORITHM = "SHA256withRSA";

    private static final String QUALIFIED_CLAIM = "%s.%s";

    private static final String TOKEN_HEADER = "{\"alg\":\"RS256\"}";

    private static final String TOKEN_PAYLOAD_FORMAT = """
            {"iss":"%s","sub":"%s","iat":%d,"exp":%d}""";

    private static final String BEARER_TOKEN_AUTHORIZATION_FORMAT = "Bearer %s.%s";

    private static final Duration EXPIRATION = Duration.ofMinutes(59);

    private static final Duration REFRESH_EXPIRATION = EXPIRATION.minusMinutes(5);

    private static final Base64.Encoder urlEncoder = Base64.getUrlEncoder().withoutPadding();

    private static final Base64.Encoder fingerprintEncoder = Base64.getEncoder();

    private final AtomicReference<RequestAuthorization> currentRequestAuthorization = new AtomicReference<>();

    private final PrivateKey privateKey;

    private final String subject;

    private final String issuer;

    /**
     * RSA Key Authorization Provider with standard properties
     *
     * @param account Snowflake Account Identifier required
     * @param user Snowflake User required
     * @param rsaPrivateKey RSA Private Key required
     */
    RSAKeyAuthorizationProvider(
            final String account,
            final String user,
            final RSAPrivateCrtKey rsaPrivateKey
    ) {
        Objects.requireNonNull(account, "Account required");
        Objects.requireNonNull(user, "User required");
        Objects.requireNonNull(rsaPrivateKey, "RSA Private Key required");

        this.subject = QUALIFIED_CLAIM.formatted(account.toUpperCase(Locale.ROOT), user.toUpperCase(Locale.ROOT));

        final PublicKey publicKey = getPublicKey(rsaPrivateKey);
        final String publicKeyFingerprint = getPublicKeyFingerprint(publicKey);

        this.issuer = QUALIFIED_CLAIM.formatted(subject, publicKeyFingerprint);
        this.privateKey = rsaPrivateKey;

        final ZonedDateTime issued = ZonedDateTime.now();
        final String authorization = getAuthorizationWithTimestamp(issued);
        final RequestAuthorization requestAuthorization = new RequestAuthorization(authorization, issued);
        this.currentRequestAuthorization.set(requestAuthorization);
    }

    /**
     * Get Request Authorization JSON Web Token signed using RSA Private Key with RS256
     *
     * @return Request Authorization with Bearer Token
     */
    RequestAuthorization getRequestAuthorization() {
        final RequestAuthorization requestAuthorization;

        final RequestAuthorization currentAuthorization = currentRequestAuthorization.get();
        if (isRefreshRequired(currentAuthorization)) {
            final ZonedDateTime issued = ZonedDateTime.now();
            final String authorization = getAuthorizationWithTimestamp(issued);
            requestAuthorization = new RequestAuthorization(authorization, issued);
            currentRequestAuthorization.set(requestAuthorization);
        } else {
            requestAuthorization = currentAuthorization;
        }

        return requestAuthorization;
    }

    private String getAuthorizationWithTimestamp(final ZonedDateTime timestamp) {
        final long issuedAt = timestamp.toEpochSecond();
        final long expiresAt = timestamp.plus(EXPIRATION).toEpochSecond();

        final String payload = TOKEN_PAYLOAD_FORMAT.formatted(issuer, subject, issuedAt, expiresAt);
        final String payloadEncoded = getObjectEncoded(payload);

        final String headerEncoded = getObjectEncoded(TOKEN_HEADER);
        final String headerPayloadEncoded = QUALIFIED_CLAIM.formatted(headerEncoded, payloadEncoded);
        final String signatureEncoded = getSignatureEncoded(headerPayloadEncoded);

        return BEARER_TOKEN_AUTHORIZATION_FORMAT.formatted(headerPayloadEncoded, signatureEncoded);
    }

    private boolean isRefreshRequired(final RequestAuthorization requestAuthorization) {
        final ZonedDateTime issued = requestAuthorization.issued();
        final ZonedDateTime refreshExpiration = issued.plus(REFRESH_EXPIRATION);
        final ZonedDateTime now = ZonedDateTime.now();
        return now.isAfter(refreshExpiration);
    }

    private String getObjectEncoded(final String object) {
        final byte[] bytes = object.getBytes(StandardCharsets.UTF_8);
        return urlEncoder.encodeToString(bytes);
    }

    private static String getPublicKeyFingerprint(final PublicKey publicKey) {
        final byte[] publicKeyDigest = getPublicKeyDigest(publicKey);
        final String publicKeyDigestEncoded = fingerprintEncoder.encodeToString(publicKeyDigest);
        return FINGERPRINT_FORMAT.formatted(publicKeyDigestEncoded);
    }

    private static byte[] getPublicKeyDigest(final PublicKey publicKey) {
        final byte[] publicKeyBinary = publicKey.getEncoded();

        try {
            final MessageDigest digest = MessageDigest.getInstance(FINGERPRINT_ALGORITHM);
            return digest.digest(publicKeyBinary);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(FINGERPRINT_ALGORITHM, e);
        }
    }

    private String getSignatureEncoded(final String headerPayloadEncoded) {
        final byte[] headerPayloadBinary = headerPayloadEncoded.getBytes(StandardCharsets.UTF_8);

        final Signature initializedSignature = getInitializedSignature();
        try {
            initializedSignature.update(headerPayloadBinary);
            final byte[] signatureBinary = initializedSignature.sign();
            return urlEncoder.encodeToString(signatureBinary);
        } catch (final SignatureException e) {
            throw new IllegalArgumentException("Signature generation failed", e);
        }
    }

    private Signature getInitializedSignature() {
        try {
            final Signature signature = Signature.getInstance(SIGNING_ALGORITHM);
            signature.initSign(privateKey);
            return signature;
        } catch (final GeneralSecurityException e) {
            throw new IllegalArgumentException("Signature initialization failed", e);
        }
    }

    private static PublicKey getPublicKey(final RSAPrivateCrtKey rsaPrivateKey) {
        try {
            final KeyFactory keyFactory = KeyFactory.getInstance(rsaPrivateKey.getAlgorithm());
            final BigInteger modulus = rsaPrivateKey.getModulus();
            final BigInteger publicExponent = rsaPrivateKey.getPublicExponent();
            final RSAPublicKeySpec publicKeySpec = new RSAPublicKeySpec(modulus, publicExponent);
            return keyFactory.generatePublic(publicKeySpec);
        } catch (final GeneralSecurityException e) {
            throw new IllegalStateException("RSA Public Key generation failed", e);
        }
    }
}
