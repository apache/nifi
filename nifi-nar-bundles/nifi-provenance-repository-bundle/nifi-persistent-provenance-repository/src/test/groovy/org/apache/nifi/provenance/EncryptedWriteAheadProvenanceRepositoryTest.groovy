/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import org.apache.nifi.events.EventReporter
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.provenance.serialization.RecordReaders
import org.apache.nifi.reporting.Severity
import org.apache.nifi.security.kms.StaticKeyProvider
import org.apache.nifi.util.file.FileUtils
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import java.security.Security
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import static org.apache.nifi.provenance.TestUtil.createFlowFile

@RunWith(JUnit4.class)
class EncryptedWriteAheadProvenanceRepositoryTest {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedWriteAheadProvenanceRepositoryTest.class)

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128
    private static final String KEY_ID = "K1"

    private static final String TRANSIT_URI = "nifi://unit-test"
    private static final String PROCESSOR_TYPE = "Mock Processor"
    private static final String COMPONENT_ID = "1234"

    private static final AtomicLong recordId = new AtomicLong()

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder()

    private ProvenanceRepository repo
    private static RepositoryConfiguration config

    public static final int DEFAULT_ROLLOVER_MILLIS = 2000
    private EventReporter eventReporter
    private List<ReportedEvent> reportedEvents = Collections.synchronizedList(new ArrayList<ReportedEvent>())

    private static String ORIGINAL_LOG_LEVEL

    @BeforeClass
    static void setUpOnce() throws Exception {
        ORIGINAL_LOG_LEVEL = System.getProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance")
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance", "DEBUG")

        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
        reportedEvents?.clear()
        eventReporter = createMockEventReporter()
    }

    @After
    void tearDown() throws Exception {
        closeRepo(repo, config)

        // Reset the boolean determiner
        RecordReaders.encryptionPropertiesRead = false
        RecordReaders.isEncryptionAvailable = false
    }

    @AfterClass
    static void tearDownOnce() throws Exception {
        if (ORIGINAL_LOG_LEVEL) {
            System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance", ORIGINAL_LOG_LEVEL)
        }
    }

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    private static RepositoryConfiguration createConfiguration() {
        RepositoryConfiguration config = new RepositoryConfiguration()
        config.addStorageDirectory("1", new File("target/storage/" + UUID.randomUUID().toString()))
        config.setCompressOnRollover(true)
        config.setMaxEventFileLife(2000L, TimeUnit.SECONDS)
        config.setCompressionBlockBytes(100)
        return config
    }

    private static RepositoryConfiguration createEncryptedConfiguration() {
        RepositoryConfiguration config = createConfiguration()
        config.setEncryptionKeys([(KEY_ID): KEY_HEX])
        config.setKeyId(KEY_ID)
        config.setKeyProviderImplementation(StaticKeyProvider.class.name)
        config
    }

    private EventReporter createMockEventReporter() {
        [reportEvent: { Severity s, String c, String m ->
            ReportedEvent event = new ReportedEvent(s, c, m)
            reportedEvents.add(event)
            logger.mock("Added ${event}")
        }] as EventReporter
    }

    private void closeRepo(ProvenanceRepository repo = this.repo, RepositoryConfiguration config = this.config) throws IOException {
        if (repo == null) {
            return
        }

        try {
            repo.close()
        } catch (IOException ioe) {
        }

        // Delete all of the storage files. We do this in order to clean up the tons of files that
        // we create but also to ensure that we have closed all of the file handles. If we leave any
        // streams open, for instance, this will throw an IOException, causing our unit test to fail.
        if (config != null) {
            for (final File storageDir : config.getStorageDirectories().values()) {
                int i
                for (i = 0; i < 3; i++) {
                    try {
                        FileUtils.deleteFile(storageDir, true)
                        break
                    } catch (IOException ioe) {
                        // if there is a virus scanner, etc. running in the background we may not be able to
                        // delete the file. Wait a sec and try again.
                        if (i == 2) {
                            throw ioe
                        } else {
                            try {
                                System.out.println("file: " + storageDir.toString() + " exists=" + storageDir.exists())
                                FileUtils.deleteFile(storageDir, true)
                                break
                            } catch (final IOException ioe2) {
                                // if there is a virus scanner, etc. running in the background we may not be able to
                                // delete the file. Wait a sec and try again.
                                if (i == 2) {
                                    throw ioe2
                                } else {
                                    try {
                                        Thread.sleep(1000L)
                                    } catch (final InterruptedException ie) {
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static
    final FlowFile buildFlowFile(Map attributes = [:], long id = recordId.getAndIncrement(), long fileSize = 3000L) {
        if (!attributes?.uuid) {
            attributes.uuid = UUID.randomUUID().toString()
        }
        createFlowFile(id, fileSize, attributes)
    }

    private
    static ProvenanceEventRecord buildEventRecord(FlowFile flowfile = buildFlowFile(), ProvenanceEventType eventType = ProvenanceEventType.RECEIVE, String transitUri = TRANSIT_URI, String componentId = COMPONENT_ID, String componentType = PROCESSOR_TYPE, long eventTime = System.currentTimeMillis()) {
        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder()
        builder.setEventTime(eventTime)
        builder.setEventType(eventType)
        builder.setTransitUri(transitUri)
        builder.fromFlowFile(flowfile)
        builder.setComponentId(componentId)
        builder.setComponentType(componentType)
        builder.build()
    }

    /**
     * This test operates on {@link WriteAheadProvenanceRepository} to verify the normal operations of existing implementations.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    void testWriteAheadProvenanceRepositoryShouldRegisterAndRetrieveEvents() throws IOException, InterruptedException {
        // Arrange
        config = createConfiguration()
        // Needed until NIFI-3605 is implemented
//        config.setMaxEventFileCapacity(1L)
        config.setMaxEventFileCount(1)
        config.setMaxEventFileLife(1, TimeUnit.SECONDS)
        repo = new WriteAheadProvenanceRepository(config)
        repo.initialize(eventReporter, null, null, IdentifierLookup.EMPTY)

        Map attributes = ["abc": "xyz",
                          "123": "456"]
        final ProvenanceEventRecord record = buildEventRecord(buildFlowFile(attributes))

        final int RECORD_COUNT = 10

        // Act
        RECORD_COUNT.times {
            repo.registerEvent(record)
        }

        // Ensure there is not a timing issue retrieving all records
        Thread.sleep(1000)

        final List<ProvenanceEventRecord> recoveredRecords = repo.getEvents(0L, RECORD_COUNT + 1)

        logger.info("Recovered ${recoveredRecords.size()} events: ")
        recoveredRecords.each { logger.info("\t${it}") }

        // Assert
        assert recoveredRecords.size() == RECORD_COUNT
        recoveredRecords.eachWithIndex { ProvenanceEventRecord recovered, int i ->
            assert recovered.getEventId() == (i as Long)
            assert recovered.getTransitUri() == TRANSIT_URI
            assert recovered.getEventType() == ProvenanceEventType.RECEIVE
            // The UUID was added later but we care that all attributes we provided are still there
            assert recovered.getAttributes().entrySet().containsAll(attributes.entrySet())
        }
    }

    @Test
    void testShouldRegisterAndGetEvent() {
        // Arrange

        // Override the boolean determiner
        RecordReaders.encryptionPropertiesRead = true
        RecordReaders.isEncryptionAvailable = true

        config = createEncryptedConfiguration()
        // Needed until NIFI-3605 is implemented
//        config.setMaxEventFileCapacity(1L)
        config.setMaxEventFileCount(1)
        config.setMaxEventFileLife(1, TimeUnit.SECONDS)
        repo = new EncryptedWriteAheadProvenanceRepository(config)
        repo.initialize(eventReporter, null, null, IdentifierLookup.EMPTY)

        Map attributes = ["abc": "This is a plaintext attribute.",
                          "123": "This is another plaintext attribute."]
        final ProvenanceEventRecord record = buildEventRecord(buildFlowFile(attributes))

        final long LAST_RECORD_ID = repo.getMaxEventId()

        // Act
        repo.registerEvent(record)

        // Retrieve the event through the interface
        ProvenanceEventRecord recoveredRecord = repo.getEvent(LAST_RECORD_ID + 1)
        logger.info("Recovered ${recoveredRecord}")

        // Assert
        assert recoveredRecord.getEventId() == LAST_RECORD_ID + 1
        assert recoveredRecord.getTransitUri() == TRANSIT_URI
        assert recoveredRecord.getEventType() == ProvenanceEventType.RECEIVE
        // The UUID was added later but we care that all attributes we provided are still there
        assert recoveredRecord.getAttributes().entrySet().containsAll(attributes.entrySet())
    }

    @Test
    void testShouldRegisterAndGetEvents() {
        // Arrange
        final int RECORD_COUNT = 10

        // Override the boolean determiner
        RecordReaders.encryptionPropertiesRead = true
        RecordReaders.isEncryptionAvailable = true

        config = createEncryptedConfiguration()
        // Needed until NIFI-3605 is implemented
//        config.setMaxEventFileCapacity(1L)
        config.setMaxEventFileCount(1)
        config.setMaxEventFileLife(1, TimeUnit.SECONDS)
        repo = new EncryptedWriteAheadProvenanceRepository(config)
        repo.initialize(eventReporter, null, null, IdentifierLookup.EMPTY)

        Map attributes = ["abc": "This is a plaintext attribute.",
                          "123": "This is another plaintext attribute."]
        final List<ProvenanceEventRecord> records = []
        RECORD_COUNT.times { int i ->
            records << buildEventRecord(buildFlowFile(attributes + [count: i as String]))
        }
        logger.info("Generated ${RECORD_COUNT} records")

        final long LAST_RECORD_ID = repo.getMaxEventId()

        // Act
        repo.registerEvents(records)
        logger.info("Registered events")

        // Retrieve the events through the interface
        List<ProvenanceEventRecord> recoveredRecords = repo.getEvents(LAST_RECORD_ID + 1, RECORD_COUNT * 2)
        logger.info("Recovered ${recoveredRecords.size()} records")

        // Assert
        recoveredRecords.eachWithIndex { ProvenanceEventRecord recoveredRecord, int i ->
            assert recoveredRecord.getEventId() == LAST_RECORD_ID + 1 + i
            assert recoveredRecord.getTransitUri() == TRANSIT_URI
            assert recoveredRecord.getEventType() == ProvenanceEventType.RECEIVE
            // The UUID was added later but we care that all attributes we provided are still there
            assert recoveredRecord.getAttributes().entrySet().containsAll(attributes.entrySet())
            assert recoveredRecord.getAttribute("count") == i as String
        }
    }

    private static class ReportedEvent {
        final Severity severity
        final String category
        final String message

        ReportedEvent(final Severity severity, final String category, final String message) {
            this.severity = severity
            this.category = category
            this.message = message
        }

        @Override
        String toString() {
            "ReportedEvent [${severity}] ${category}: ${message}"
        }
    }
}
