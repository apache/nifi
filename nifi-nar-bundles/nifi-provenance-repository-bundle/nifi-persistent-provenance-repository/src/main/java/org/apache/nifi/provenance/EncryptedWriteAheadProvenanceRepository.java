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
import java.util.Map;
import java.util.stream.Collectors;
import javax.crypto.SecretKey;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.store.EventFileManager;
import org.apache.nifi.provenance.store.RecordReaderFactory;
import org.apache.nifi.provenance.store.RecordWriterFactory;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
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
                KeyProvider keyProvider = buildKeyProvider();
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
        RepositoryConfiguration config = super.getConfig();
        if (config == null) {
            throw new KeyManagementException("The repository configuration is missing");
        }

        final String implementationClassName = config.getKeyProviderImplementation();
        if (implementationClassName == null) {
            throw new KeyManagementException("Cannot create Key Provider because the NiFi Properties is missing the following property: "
                    + NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS);
        }

        // TODO: Extract to factory
        KeyProvider keyProvider;
        if (StaticKeyProvider.class.getName().equals(implementationClassName)) {
            // Get all the keys (map) from config
            if (CryptoUtils.isValidKeyProvider(implementationClassName, config.getKeyProviderLocation(), config.getKeyId(), config.getEncryptionKeys())) {
                Map<String, SecretKey> formedKeys = config.getEncryptionKeys().entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> {
                                    try {
                                        return CryptoUtils.formKeyFromHex(e.getValue());
                                    } catch (KeyManagementException e1) {
                                        // This should never happen because the hex has already been validated
                                        logger.error("Encountered an error: ", e1);
                                        return null;
                                    }
                                }));
                keyProvider = new StaticKeyProvider(formedKeys);
            } else {
                final String msg = "The StaticKeyProvider definition is not valid";
                logger.error(msg);
                throw new KeyManagementException(msg);
            }
        } else if (FileBasedKeyProvider.class.getName().equals(implementationClassName)) {
            keyProvider = new FileBasedKeyProvider(config.getKeyProviderLocation());
            if (!keyProvider.keyExists(config.getKeyId())) {
                throw new KeyManagementException("The specified key ID " + config.getKeyId() + " is not in the key definition file");
            }
        } else {
            throw new KeyManagementException("Invalid key provider implementation provided: " + implementationClassName);
        }

        return keyProvider;
    }
}
