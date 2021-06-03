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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.store.EventFileManager;
import org.apache.nifi.provenance.store.RecordReaderFactory;
import org.apache.nifi.provenance.store.RecordWriterFactory;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.security.repository.RepositoryEncryptorUtils;
import org.apache.nifi.security.repository.config.ProvenanceRepositoryEncryptionConfiguration;
import org.apache.nifi.security.repository.config.RepositoryEncryptionConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;

/**
 * This class is an implementation of the {@link WriteAheadProvenanceRepository} provenance repository which provides transparent
 * block encryption/decryption of provenance event data during file system interaction. As of Apache NiFi 1.10.0
 * (October 2019), this implementation is considered <a href="https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#experimental-warning">*experimental*</a>. For further details, review the
 * <a href="https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#encrypted-provenance">Apache NiFi User Guide -
 * Encrypted Provenance Repository</a> and
 * <a href="https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#encrypted-write-ahead-provenance-repository-properties">Apache NiFi Admin Guide - Encrypted Write-Ahead Provenance
 * Repository Properties</a>.
 */
public class EncryptedWriteAheadProvenanceRepository extends WriteAheadProvenanceRepository {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedWriteAheadProvenanceRepository.class);

    /**
     * This constructor exists solely for the use of the Java Service Loader mechanism and should not be used.
     */
    @SuppressWarnings("unused")
    public EncryptedWriteAheadProvenanceRepository() {
        super();
    }

    // Created via reflection from FlowController
    @SuppressWarnings("unused")
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
                final KeyProvider keyProvider = buildKeyProvider();
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

    private KeyProvider buildKeyProvider() throws IOException {
        final RepositoryConfiguration config = getConfig();
        final RepositoryEncryptionConfiguration configuration = new ProvenanceRepositoryEncryptionConfiguration(
                config.getKeyProviderImplementation(),
                config.getKeyProviderLocation(),
                config.getKeyId(),
                config.getEncryptionKeys(),
                getClass().getName(),
                config.getKeyProviderPassword()
        );
        return RepositoryEncryptorUtils.validateAndBuildRepositoryKeyProvider(configuration);
    }
}
