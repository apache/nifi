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
package org.apache.nifi.key.service.reader;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.security.PrivateKey;

/**
 * Bouncy Castle implementation of Private Key Reader supporting PEM files
 */
public class BouncyCastlePrivateKeyReader implements PrivateKeyReader {
    private static final String INVALID_PEM = "Invalid PEM";

    private static final BouncyCastleProvider BOUNCY_CASTLE_PROVIDER = new BouncyCastleProvider();

    /**
     * Read Private Key using Bouncy Castle PEM Parser
     *
     * @param inputStream Key stream
     * @param keyPassword Password
     * @return Private Key
     */
    @Override
    public PrivateKey readPrivateKey(final InputStream inputStream, final char[] keyPassword) {
        try (final PEMParser parser = new PEMParser(new InputStreamReader(inputStream))) {
            final Object object = parser.readObject();

            final PrivateKeyInfo privateKeyInfo;

            if (object instanceof PrivateKeyInfo) {
                privateKeyInfo = (PrivateKeyInfo) object;
            } else if (object instanceof PKCS8EncryptedPrivateKeyInfo) {
                final PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) object;
                privateKeyInfo = readEncryptedPrivateKey(encryptedPrivateKeyInfo, keyPassword);
            } else if (object instanceof PEMKeyPair) {
                final PEMKeyPair pemKeyPair = (PEMKeyPair) object;
                privateKeyInfo = pemKeyPair.getPrivateKeyInfo();
            } else if (object instanceof PEMEncryptedKeyPair) {
                final PEMEncryptedKeyPair encryptedKeyPair = (PEMEncryptedKeyPair) object;
                privateKeyInfo = readEncryptedPrivateKey(encryptedKeyPair, keyPassword);
            } else {
                final String objectType = object == null ? INVALID_PEM : object.getClass().getName();
                final String message = String.format("Private Key [%s] not supported", objectType);
                throw new IllegalArgumentException(message);
            }

            return convertPrivateKey(privateKeyInfo);
        } catch (final IOException e) {
            throw new UncheckedIOException("Read Private Key stream failed", e);
        }
    }

    private PrivateKeyInfo readEncryptedPrivateKey(final PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo, final char[] keyPassword) {
        try {
            final InputDecryptorProvider provider = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                    .setProvider(BOUNCY_CASTLE_PROVIDER)
                    .build(keyPassword);
            return encryptedPrivateKeyInfo.decryptPrivateKeyInfo(provider);
        } catch (final OperatorCreationException e) {
            throw new PrivateKeyException("Preparing Private Key Decryption failed", e);
        } catch (final PKCSException e) {
            throw new PrivateKeyException("Decrypting Private Key failed", e);
        }
    }

    private PrivateKeyInfo readEncryptedPrivateKey(final PEMEncryptedKeyPair encryptedKeyPair, final char[] keyPassword) {
        final PEMDecryptorProvider provider = new JcePEMDecryptorProviderBuilder()
                .setProvider(BOUNCY_CASTLE_PROVIDER)
                .build(keyPassword);
        try {
            final PEMKeyPair pemKeyPair = encryptedKeyPair.decryptKeyPair(provider);
            return pemKeyPair.getPrivateKeyInfo();
        } catch (final IOException e) {
            throw new PrivateKeyException("Decrypting Private Key Pair failed", e);
        }
    }

    private PrivateKey convertPrivateKey(final PrivateKeyInfo privateKeyInfo) {
        final JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
        try {
            return converter.getPrivateKey(privateKeyInfo);
        } catch (final PEMException e) {
            throw new PrivateKeyException("Convert Private Key failed", e);
        }
    }
}
