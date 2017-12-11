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
package org.apache.nifi.provenance;

import java.io.IOException;
import java.security.KeyManagementException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.store.EventFileManager;
import org.apache.nifi.provenance.store.RecordReaderFactory;
import org.apache.nifi.provenance.store.RecordWriterFactory;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.security.kms.KeyProviderFactory;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptedWriteAheadProvenanceRepository extends WriteAheadProvenanceRepository {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedWriteAheadProvenanceRepository.class);

    /**
     * This constructor exists solely for the use of the Java Service Loader mechanism and should not be used.
     */
    public EncryptedWriteAheadProvenanceRepository() {
        super();
    }

    public EncryptedWriteAheadProvenanceRepository(final NiFiProperties nifiProperties) {
        super(RepositoryConfiguration.create(nifiProperties));
    }

    public EncryptedWriteAheadProvenanceRepository(final RepositoryConfiguration config) {
        super(config);
    }

    /**
     * This method initializes the repository. It first builds the key provider and event encryptor
     * from the config values, then creates the encrypted record writer and reader, then delegates
     * back to the superclass for the common implementation.
     *
     * @param eventReporter   the event reporter
     * @param authorizer      the authorizer
     * @param resourceFactory the authorizable factory
     * @param idLookup        the lookup provider
     * @throws IOException if there is an error initializing this repository
     */
    @Override
    public synchronized void initialize(final EventReporter eventReporter, final Authorizer authorizer, final ProvenanceAuthorizableFactory resourceFactory,
                                        final IdentifierLookup idLookup) throws IOException {
        // Initialize the encryption-specific fields
        ProvenanceEventEncryptor provenanceEventEncryptor;
        if (getConfig().supportsEncryption()) {
            try {
                KeyProvider keyProvider;
                if (KeyProviderFactory.requiresMasterKey(getConfig().getKeyProviderImplementation())) {
                    SecretKey masterKey = getMasterKey();
                    keyProvider = buildKeyProvider(masterKey);
                } else {
                    keyProvider = buildKeyProvider();
                }
                provenanceEventEncryptor = new AESProvenanceEventEncryptor();
                provenanceEventEncryptor.initialize(keyProvider);
            } catch (KeyManagementException e) {
                String msg = "Encountered an error building the key provider";
                logger.error(msg, e);
                throw new IOException(msg, e);
            }
        } else {
            throw new IOException("The provided configuration does not support a encrypted repository");
        }

        // Build a factory using lambda which injects the encryptor
        final RecordWriterFactory recordWriterFactory = (file, idGenerator, compressed, createToc) -> {
            try {
                final TocWriter tocWriter = createToc ? new StandardTocWriter(TocUtil.getTocFile(file), false, false) : null;
                return new EncryptedSchemaRecordWriter(file, idGenerator, tocWriter, compressed, BLOCK_SIZE, idLookup, provenanceEventEncryptor, getConfig().getDebugFrequency());
            } catch (EncryptionException e) {
                logger.error("Encountered an error building the schema record writer factory: ", e);
                throw new IOException(e);
            }
        };

        // Build a factory using lambda which injects the encryptor
        final EventFileManager fileManager = new EventFileManager();
        final RecordReaderFactory recordReaderFactory = (file, logs, maxChars) -> {
            fileManager.obtainReadLock(file);
            try {
                EncryptedSchemaRecordReader tempReader = (EncryptedSchemaRecordReader) RecordReaders.newRecordReader(file, logs, maxChars);
                tempReader.setProvenanceEventEncryptor(provenanceEventEncryptor);
                return tempReader;
            } finally {
                fileManager.releaseReadLock(file);
            }
        };

        // Delegate the init to the parent impl
        super.init(recordWriterFactory, recordReaderFactory, eventReporter, authorizer, resourceFactory);
    }

    private KeyProvider buildKeyProvider() throws KeyManagementException {
        return buildKeyProvider(null);
    }

    private KeyProvider buildKeyProvider(SecretKey masterKey) throws KeyManagementException {
        RepositoryConfiguration config = super.getConfig();
        if (config == null) {
            throw new KeyManagementException("The repository configuration is missing");
        }

        final String implementationClassName = config.getKeyProviderImplementation();
        if (implementationClassName == null) {
            throw new KeyManagementException("Cannot create Key Provider because the NiFi Properties is missing the following property: "
                    + NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS);
        }

        return KeyProviderFactory.buildKeyProvider(implementationClassName, config.getKeyProviderLocation(), config.getKeyId(), config.getEncryptionKeys(), masterKey);
    }

    private static SecretKey getMasterKey() throws KeyManagementException {
        try {
            // Get the master encryption key from bootstrap.conf
            String masterKeyHex = NiFiPropertiesLoader.extractKeyFromBootstrapFile();
            return new SecretKeySpec(Hex.decodeHex(masterKeyHex.toCharArray()), "AES");
        } catch (IOException | DecoderException e) {
            logger.error("Encountered an error: ", e);
            throw new KeyManagementException(e);
        }
    }
}
