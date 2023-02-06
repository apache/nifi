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
package org.apache.nifi.provenance

import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.provenance.serialization.RecordReader
import org.apache.nifi.provenance.serialization.RecordWriter
import org.apache.nifi.provenance.toc.StandardTocReader
import org.apache.nifi.provenance.toc.StandardTocWriter
import org.apache.nifi.provenance.toc.TocReader
import org.apache.nifi.provenance.toc.TocUtil
import org.apache.nifi.provenance.toc.TocWriter
import org.apache.nifi.repository.encryption.AesGcmByteArrayRepositoryEncryptor
import org.apache.nifi.repository.encryption.RepositoryEncryptor
import org.apache.nifi.repository.encryption.configuration.EncryptionMetadataHeader
import org.apache.nifi.security.kms.KeyProvider
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.encoders.Hex
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.spec.SecretKeySpec
import java.security.KeyManagementException
import java.security.Security
import java.util.concurrent.atomic.AtomicLong

import static org.apache.nifi.provenance.TestUtil.createFlowFile
import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertThrows

class EncryptedSchemaRecordReaderWriterTest extends AbstractTestRecordReaderWriter {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSchemaRecordReaderWriterTest.class)

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX = KEY_HEX_128
    private static final String KEY_ID = "K1"

    private static final String TRANSIT_URI = "nifi://unit-test"
    private static final String PROCESSOR_TYPE = "Mock Processor"
    private static final String COMPONENT_ID = "1234"

    private static final int UNCOMPRESSED_BLOCK_SIZE = 1024 * 32
    private static final int MAX_ATTRIBUTE_SIZE = 2048

    private static final AtomicLong idGenerator = new AtomicLong(0L)
    private File journalFile
    private File tocFile

    private static KeyProvider mockKeyProvider
    private static RepositoryEncryptor<byte[], byte[]> repositoryEncryptor

    @BeforeAll
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        mockKeyProvider = [
                getKey            : { String keyId ->
                    if (keyId == KEY_ID) {
                        new SecretKeySpec(Hex.decode(KEY_HEX), "AES")
                    } else {
                        throw new KeyManagementException("${keyId} is not available")
                    }
                },
                getAvailableKeyIds: { ->
                    [KEY_ID]
                },
                keyExists         : { String keyId ->
                    keyId == KEY_ID
                }] as KeyProvider
        repositoryEncryptor = new AesGcmByteArrayRepositoryEncryptor(mockKeyProvider, EncryptionMetadataHeader.PROVENANCE)
    }

    @BeforeEach
    void setUp() throws Exception {
        journalFile = File.createTempFile(getClass().simpleName, ".journal")
        journalFile.deleteOnExit()
        tocFile = TocUtil.getTocFile(journalFile)
        idGenerator.set(0L)
    }

    private static
    final FlowFile buildFlowFile(Map attributes = [:], long id = idGenerator.getAndIncrement(),
                                 long fileSize = 3000L) {
        if (!attributes?.uuid) {
            attributes.uuid = UUID.randomUUID().toString()
        }
        createFlowFile(id, fileSize, attributes)
    }

    private static ProvenanceEventRecord buildEventRecord(FlowFile flowfile = buildFlowFile(),
                                                  ProvenanceEventType eventType = ProvenanceEventType.RECEIVE,
                                                  String transitUri = TRANSIT_URI, String componentId = COMPONENT_ID,
                                                  String componentType = PROCESSOR_TYPE,
                                                  long eventTime = System.currentTimeMillis()) {
        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder()
        builder.setEventTime(eventTime)
        builder.setEventType(eventType)
        builder.setTransitUri(transitUri)
        builder.fromFlowFile(flowfile)
        builder.setComponentId(componentId)
        builder.setComponentType(componentType)
        builder.build()
    }

    @Override
    protected RecordWriter createWriter(
            final File file,
            final TocWriter tocWriter, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        createWriter(file, tocWriter, compressed, uncompressedBlockSize, repositoryEncryptor)
    }

    protected static RecordWriter createWriter(
            final File file,
            final TocWriter tocWriter,
            final boolean compressed,
            final int uncompressedBlockSize, RepositoryEncryptor<byte[], byte[]> encryptor) throws IOException {
        return new EncryptedSchemaRecordWriter(file, idGenerator, tocWriter, compressed, uncompressedBlockSize, IdentifierLookup.EMPTY, encryptor, KEY_ID)
    }

    @Override
    protected RecordReader createReader(
            final InputStream inputStream,
            final String journalFilename, final TocReader tocReader, final int maxAttributeSize) throws IOException {
        return new EncryptedSchemaRecordReader(inputStream, journalFilename, tocReader, maxAttributeSize, repositoryEncryptor)
    }

    /**
     * Build a record and write it to the repository with the encrypted writer. Recover with the encrypted reader and verify.
     */
    @Test
    void testShouldWriteAndReadEncryptedRecord() {
        // Arrange
        final ProvenanceEventRecord record = buildEventRecord()
        logger.info("Built sample PER: ${record}")

        TocWriter tocWriter = new StandardTocWriter(tocFile, false, false)

        RecordWriter encryptedWriter = createWriter(journalFile, tocWriter, false, UNCOMPRESSED_BLOCK_SIZE)
        logger.info("Generated encrypted writer: ${encryptedWriter}")

        // Act
        long encryptedRecordId = idGenerator.get()
        encryptedWriter.writeHeader(encryptedRecordId)
        encryptedWriter.writeRecords(Collections.singletonList(record))
        encryptedWriter.close()
        logger.info("Wrote encrypted record ${encryptedRecordId} to journal")

        // Assert
        TocReader tocReader = new StandardTocReader(tocFile)
        final FileInputStream fis = new FileInputStream(journalFile)
        final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, MAX_ATTRIBUTE_SIZE)
        logger.info("Generated encrypted reader: ${reader}")

        ProvenanceEventRecord encryptedEvent = reader.nextRecord()
        assertNotNull(encryptedEvent)
        assertEquals(encryptedRecordId, encryptedEvent.getEventId())
        assertEquals(record.componentId, encryptedEvent.getComponentId())
        assertEquals(record.componentType, encryptedEvent.getComponentType())
        logger.info("Successfully read encrypted record: ${encryptedEvent}")

        assertNull(reader.nextRecord())
    }

    /**
     * Build a record and write it with a standard writer and the encrypted writer to different repositories.
     * Recover with the standard reader and the contents of the encrypted record should be unreadable.
     */
    @Test
    void testShouldWriteEncryptedRecordAndPlainRecord() {
        // Arrange
        final ProvenanceEventRecord record = buildEventRecord()
        logger.info("Built sample PER: ${record}")

        TocWriter tocWriter = new StandardTocWriter(tocFile, false, false)

        RecordWriter standardWriter = new EventIdFirstSchemaRecordWriter(journalFile, idGenerator, tocWriter, false, UNCOMPRESSED_BLOCK_SIZE, IdentifierLookup.EMPTY)
        logger.info("Generated standard writer: ${standardWriter}")

        File encryptedJournalFile = new File(journalFile.absolutePath + "_encrypted")
        File encryptedTocFile = TocUtil.getTocFile(encryptedJournalFile)
        TocWriter encryptedTocWriter = new StandardTocWriter(encryptedTocFile, false, false)
        RecordWriter encryptedWriter = createWriter(encryptedJournalFile, encryptedTocWriter, false, UNCOMPRESSED_BLOCK_SIZE)
        logger.info("Generated encrypted writer: ${encryptedWriter}")

        // Act
        long standardRecordId = idGenerator.get()
        standardWriter.writeHeader(standardRecordId)
        standardWriter.writeRecords(Collections.singletonList(record))
        standardWriter.close()
        logger.info("Wrote standard record ${standardRecordId} to journal")

        long encryptedRecordId = idGenerator.get()
        encryptedWriter.writeHeader(encryptedRecordId)
        encryptedWriter.writeRecords(Collections.singletonList(record))
        encryptedWriter.close()
        logger.info("Wrote encrypted record ${encryptedRecordId} to journal")

        // Assert
        TocReader tocReader = new StandardTocReader(tocFile)
        final FileInputStream fis = new FileInputStream(journalFile)
        final RecordReader reader = new EventIdFirstSchemaRecordReader(fis, journalFile.getName(), tocReader, MAX_ATTRIBUTE_SIZE)
        logger.info("Generated standard reader: ${reader}")

        ProvenanceEventRecord standardEvent = reader.nextRecord()
        assertNotNull(standardEvent)
        assertEquals(standardRecordId, standardEvent.getEventId())
        assertEquals(record.componentId, standardEvent.getComponentId())
        assertEquals(record.componentType, standardEvent.getComponentType())
        logger.info("Successfully read standard record: ${standardEvent}")

        assertNull(reader.nextRecord())

        // Demonstrate unable to read from encrypted file with standard reader
        TocReader incompatibleTocReader = new StandardTocReader(encryptedTocFile)
        final FileInputStream efis = new FileInputStream(encryptedJournalFile)
        RecordReader incompatibleReader = new EventIdFirstSchemaRecordReader(efis, encryptedJournalFile.getName(), incompatibleTocReader, MAX_ATTRIBUTE_SIZE)
        logger.info("Generated standard reader (attempting to read encrypted file): ${incompatibleReader}")

        assertThrows(EOFException.class, () -> incompatibleReader.nextRecord())
    }
}
