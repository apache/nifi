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
import org.apache.nifi.reporting.Severity
import org.apache.nifi.security.kms.StaticKeyProvider
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.file.FileUtils
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import java.security.Security
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import static org.apache.nifi.provenance.TestUtil.createFlowFile
import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.CoreMatchers.is
import static org.hamcrest.CoreMatchers.hasItems

class EncryptedWriteAheadProvenanceRepositoryTest {
    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX = KEY_HEX_128
    private static final String KEY_ID = "K1"

    private static final String TRANSIT_URI = "nifi://unit-test"
    private static final String PROCESSOR_TYPE = "Mock Processor"
    private static final String COMPONENT_ID = "1234"

    private static final AtomicLong recordId = new AtomicLong()

    private ProvenanceRepository repo
    private static RepositoryConfiguration config
    private File provenanceRepositoryDirectory

    private EventReporter eventReporter
    private List<ReportedEvent> reportedEvents = Collections.synchronizedList(new ArrayList<ReportedEvent>())

    @BeforeAll
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())
    }

    @BeforeEach
    void setUp() throws Exception {
        provenanceRepositoryDirectory = File.createTempDir(getClass().simpleName)
        reportedEvents?.clear()
        eventReporter = createMockEventReporter()
    }

    @AfterEach
    void tearDown() throws Exception {
        closeRepo(repo, config)
        if (provenanceRepositoryDirectory != null & provenanceRepositoryDirectory.isDirectory()) {
            provenanceRepositoryDirectory.deleteDir()
        }
    }

    private static RepositoryConfiguration createConfiguration(final File provenanceDir) {
        final RepositoryConfiguration config = new RepositoryConfiguration()
        config.addStorageDirectory("1", provenanceDir)
        config.setCompressOnRollover(true)
        config.setMaxEventFileLife(2000L, TimeUnit.SECONDS)
        config.setCompressionBlockBytes(100)
        return config
    }

    private EventReporter createMockEventReporter() {
        [reportEvent: { Severity s, String c, String m ->
            ReportedEvent event = new ReportedEvent(s, c, m)
            reportedEvents.add(event)
        }] as EventReporter
    }

    private void closeRepo(final ProvenanceRepository repo = this.repo, final RepositoryConfiguration config = this.config) throws IOException {
        if (repo == null) {
            return
        }

        try {
            repo.close()
        } catch (final IOException ignored) {
            // intentionally blank
        }

        // Delete all of the storage files. We do this in order to clean up the tons of files that
        // we create but also to ensure that we have closed all of the file handles. If we leave any
        // streams open, for instance, this will throw an IOException, causing our unit test to fail.
        if (config != null) {
            for (final File storageDir : config.getStorageDirectories().values()) {
                for (int i = 0; i < 3; i++) {
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
                                    } catch (final InterruptedException ignored) {
                                        // intentionally blank
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static final FlowFile buildFlowFile(final Map attributes = [:], final long id = recordId.getAndIncrement(), final long fileSize = 3000L) {
        if (!attributes?.uuid) {
            attributes.uuid = UUID.randomUUID().toString()
        }
        createFlowFile(id, fileSize, attributes)
    }

    private static ProvenanceEventRecord buildEventRecord(final FlowFile flowfile = buildFlowFile(), final ProvenanceEventType eventType = ProvenanceEventType.RECEIVE,
                                                          final String transitUri = TRANSIT_URI, final String componentId = COMPONENT_ID,
                                                          final String componentType = PROCESSOR_TYPE, final long eventTime = System.currentTimeMillis()) {
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
        config = createConfiguration(provenanceRepositoryDirectory)
        // Needed until NIFI-3605 is implemented
//        config.setMaxEventFileCapacity(1L)
        config.setMaxEventFileCount(1)
        config.setMaxEventFileLife(1, TimeUnit.SECONDS)
        repo = new WriteAheadProvenanceRepository(config)
        repo.initialize(eventReporter, null, null, IdentifierLookup.EMPTY)

        final Map attributes = ["abc": "xyz",
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

        // Assert
        assertThat(recoveredRecords.size(), is(RECORD_COUNT))
        recoveredRecords.eachWithIndex { ProvenanceEventRecord recovered, int i ->
            assertThat(recovered.getEventId(), is(i as Long))
            assertThat(recovered.getTransitUri(), is(TRANSIT_URI))
            assertThat(recovered.getEventType(), is(ProvenanceEventType.RECEIVE))
            // The UUID was added later but we care that all attributes we provided are still there
            assertThat(recovered.getAttributes().entrySet(), hasItems(attributes.entrySet().toArray() as Map.Entry<String, String>[]))
        }
    }

    @Test
    void testEncryptedWriteAheadProvenanceRepositoryShouldRegisterAndGetEvents() {
        // Arrange
        final int RECORD_COUNT = 10

        // ensure NiFiProperties are converted to RepositoryConfig during encrypted repo constructor
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, [
                (NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS): StaticKeyProvider.class.name,
                (NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY): KEY_HEX,
                (NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_ID): KEY_ID,
                (NiFiProperties.PROVENANCE_REPO_DIRECTORY_PREFIX + "test"): provenanceRepositoryDirectory.toString()
        ])

        repo = new EncryptedWriteAheadProvenanceRepository(properties)
        config = repo.getConfig()
        repo.initialize(eventReporter, null, null, IdentifierLookup.EMPTY)

        final Map attributes = ["abc": "This is a plaintext attribute.",
                                "123": "This is another plaintext attribute."]
        final List<ProvenanceEventRecord> records = []
        RECORD_COUNT.times { int i ->
            records << buildEventRecord(buildFlowFile(attributes + [count: i as String]))
        }

        final long LAST_RECORD_ID = repo.getMaxEventId()

        // Act
        repo.registerEvents(records)

        // Retrieve the events through the interface
        final List<ProvenanceEventRecord> recoveredRecords = repo.getEvents(LAST_RECORD_ID + 1, RECORD_COUNT * 2)

        // Assert
        recoveredRecords.eachWithIndex { ProvenanceEventRecord recoveredRecord, int i ->
            assertThat(recoveredRecord.getEventId(), is(LAST_RECORD_ID + 1 + i))
            assertThat(recoveredRecord.getTransitUri(), is(TRANSIT_URI))
            assertThat(recoveredRecord.getEventType(), is(ProvenanceEventType.RECEIVE))
            // The UUID was added later but we care that all attributes we provided are still there
            assertThat(recoveredRecord.getAttributes().entrySet(), hasItems((Map.Entry<String, String>[])attributes.entrySet().toArray()))
            assertThat(recoveredRecord.getAttribute("count"), is(i as String))
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
