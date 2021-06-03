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
package org.apache.nifi.controller;

import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.security.kms.EncryptionException;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.security.repository.RepositoryEncryptorUtils;
import org.apache.nifi.security.repository.config.FlowFileRepositoryEncryptionConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

/**
 * <p>
 * An implementation of the {@link FlowFileSwapManager} that swaps FlowFiles
 * to/from local disk.  The swap file is encrypted using AES/CTR, using the
 * encryption key defined in nifi.properties.
 * </p>
 */
@SuppressWarnings("unused")  // instantiation via reflection in product
public class FileSystemSwapManagerEncrypt extends FileSystemSwapManager {

    private static final Logger logger = LoggerFactory.getLogger(FileSystemSwapManagerEncrypt.class);

    private final SecretKey secretKey;

    /**
     * Default no args constructor for service loading only.
     */
    public FileSystemSwapManagerEncrypt() {
        throw new IllegalStateException("ctor not supported; nifi properties not available");
    }

    public FileSystemSwapManagerEncrypt(final NiFiProperties nifiProperties)
            throws IOException, EncryptionException, GeneralSecurityException {
        super(nifiProperties);
        // acquire reference to FlowFileRepository key
        final FlowFileRepositoryEncryptionConfiguration configuration = new FlowFileRepositoryEncryptionConfiguration(nifiProperties);
        if (!CryptoUtils.isValidRepositoryEncryptionConfiguration(configuration)) {
            logger.error("The flowfile repository encryption configuration is not valid (see above). Shutting down...");
            throw new EncryptionException("The flowfile repository encryption configuration is not valid");
        }
        final KeyProvider keyProvider = RepositoryEncryptorUtils.validateAndBuildRepositoryKeyProvider(configuration);
        this.secretKey = keyProvider.getKey(configuration.getEncryptionKeyId());
    }

    public FileSystemSwapManagerEncrypt(final Path flowFileRepoPath) {
        throw new IllegalStateException("ctor not supported; nifi properties not available");
    }

    protected InputStream getInputStream(final File file) throws IOException {
        final FileInputStream fis = new FileInputStream(file);
        try {
            final byte[] iv = new byte[SIZE_IV_AES_BYTES];
            final int countIV = fis.read(iv);
            if (countIV != SIZE_IV_AES_BYTES) {
                throw new IOException(String.format(
                        "problem reading IV [expected=%d, actual=%d]", SIZE_IV_AES_BYTES, countIV));
            }
            final Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(SIZE_TAG_GCM_BITS, iv));
            return new CipherInputStream(fis, cipher);
        } catch (GeneralSecurityException e) {
            throw new IOException(e);
        }
    }

    protected OutputStream getOutputStream(final File file) throws IOException {
        final byte[] iv = new byte[SIZE_IV_AES_BYTES];
        new SecureRandom().nextBytes(iv);
        final FileOutputStream fos = new FileOutputStream(file);
        fos.write(iv);
        try {
            final Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(SIZE_TAG_GCM_BITS, iv));
            return new CipherOutputStream(fos, cipher);
        } catch (GeneralSecurityException e) {
            throw new IOException(e);
        }
    }

    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int SIZE_IV_AES_BYTES = 16;
    private static final int SIZE_TAG_GCM_BITS = 128;
}
