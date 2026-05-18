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
package org.apache.nifi.processors.aws.s3;

import org.apache.nifi.components.Backlog;
import org.apache.nifi.components.BacklogReportingException;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.BacklogReportingProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Provides integration level testing with actual AWS S3 resources for {@link ListS3} and requires additional configuration and resources to work.
 */
public class ITListS3 extends AbstractS3IT {
    @Test
    public void testSimpleList() throws IOException, InitializationException {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "a");
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
    }

    @Test
    public void testSimpleListUsingCredentialsProviderService() throws Throwable {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "a");
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
    }

    @Test
    public void testSimpleListWithDelimiter() throws Throwable {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.setProperty(ListS3.DELIMITER, "/");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "a");
    }

    @Test
    public void testSimpleListWithPrefix() throws Throwable {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.setProperty(ListS3.PREFIX, "b/");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "b/c");
    }

    @Test
    public void testSimpleListWithPrefixAndVersions() throws Throwable {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.setProperty(ListS3.PREFIX, "b/");
        runner.setProperty(ListS3.USE_VERSIONS, "true");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "b/c");
    }

    @Test
    public void testObjectTagsWritten() throws InitializationException {
        List<Tag> objectTags = new ArrayList<>();
        objectTags.add(Tag.builder().key("dummytag1").value("dummyvalue1").build());
        objectTags.add(Tag.builder().key("dummytag2").value("dummyvalue2").build());

        putFileWithObjectTag("t/fileWithTag", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME), objectTags);
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.setProperty(ListS3.PREFIX, "t/");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        runner.setProperty(ListS3.WRITE_OBJECT_TAGS, "true");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);

        MockFlowFile flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS).get(0);

        flowFiles.assertAttributeEquals("filename", "t/fileWithTag");
        flowFiles.assertAttributeExists("s3.tag.dummytag1");
        flowFiles.assertAttributeExists("s3.tag.dummytag2");
        flowFiles.assertAttributeEquals("s3.tag.dummytag1", "dummyvalue1");
        flowFiles.assertAttributeEquals("s3.tag.dummytag2", "dummyvalue2");
    }

    @Test
    public void testUserMetadataWritten() throws FileNotFoundException, InitializationException {
        Map<String, String> userMetadata = new HashMap<>();
        userMetadata.put("dummy.metadata.1", "dummyvalue1");
        userMetadata.put("dummy.metadata.2", "dummyvalue2");

        putFileWithUserMetadata("m/fileWithUserMetadata", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME), userMetadata);
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.setProperty(ListS3.PREFIX, "m/");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        runner.setProperty(ListS3.WRITE_USER_METADATA, "true");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);

        MockFlowFile flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS).get(0);

        flowFiles.assertAttributeEquals("filename", "m/fileWithUserMetadata");
        flowFiles.assertAttributeExists("s3.user.metadata.dummy.metadata.1");
        flowFiles.assertAttributeExists("s3.user.metadata.dummy.metadata.2");
        flowFiles.assertAttributeEquals("s3.user.metadata.dummy.metadata.1", "dummyvalue1");
        flowFiles.assertAttributeEquals("s3.user.metadata.dummy.metadata.2", "dummyvalue2");
    }

    @Test
    public void testNoTrackingBacklogDoesNotListS3() throws BacklogReportingException, InitializationException {
        final S3Client spyClient = spy(getClient());
        final TestRunner runner = initBacklogRunner(new TestableListS3(spyClient));
        runner.setProperty(ListS3.LISTING_STRATEGY, ListS3.NO_TRACKING.getValue());
        runner.run(0, false, true);

        clearInvocations(spyClient);

        final Backlog backlog = reportBacklog(runner).orElseThrow();
        assertTrue(backlog.getFlowFileCount().isPresent());
        assertEquals(0L, backlog.getFlowFileCount().getAsLong());
        assertTrue(backlog.getByteCount().isPresent());
        assertEquals(0L, backlog.getByteCount().getAsLong());
        assertTrue(backlog.getRecordCount().isPresent());
        assertEquals(0L, backlog.getRecordCount().getAsLong());
        assertEquals(Backlog.Precision.EXACT, backlog.getPrecision());
        assertTrue(backlog.getLastCaughtUp().isPresent());
        verifyNoInteractions(spyClient);
    }

    @Test
    public void testTimestampTrackingBacklogUsesPersistedWatermark() throws BacklogReportingException, InitializationException {
        putTestFile("timestamps/first.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("timestamps/second.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final S3Client spyClient = spy(getClient());
        final TestRunner runner = initBacklogRunner(new TestableListS3(spyClient));
        runner.setProperty(ListS3.LISTING_STRATEGY, ListS3.BY_TIMESTAMPS.getValue());
        runner.setProperty(ListS3.PREFIX, "timestamps/");
        runner.run(0, false, true);

        final Backlog firstRunBacklog = reportBacklog(runner).orElseThrow();
        assertEquals(Backlog.Precision.EXACT, firstRunBacklog.getPrecision());
        assertTrue(firstRunBacklog.getFlowFileCount().isPresent());
        assertEquals(2L, firstRunBacklog.getFlowFileCount().getAsLong());
        assertFalse(firstRunBacklog.getLastCaughtUp().isPresent());

        runner.run(1, false, false);

        putTestFile("timestamps/third.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final Backlog backlog = reportBacklog(runner).orElseThrow();
        assertEquals(Backlog.Precision.EXACT, backlog.getPrecision());
        assertTrue(backlog.getFlowFileCount().isPresent());
        assertEquals(1L, backlog.getFlowFileCount().getAsLong());
        assertFalse(backlog.getLastCaughtUp().isPresent());
        verify(spyClient, atLeastOnce()).listObjectsV2(any(ListObjectsV2Request.class));

        runner.run(1, false, false);

        final Backlog caughtUpBacklog = reportBacklog(runner).orElseThrow();
        assertEquals(Backlog.Precision.EXACT, caughtUpBacklog.getPrecision());
        assertTrue(caughtUpBacklog.getFlowFileCount().isPresent());
        assertEquals(0L, caughtUpBacklog.getFlowFileCount().getAsLong());
        assertTrue(caughtUpBacklog.getLastCaughtUp().isPresent());
    }

    @Test
    public void testEntityTrackingBacklogUsesListedEntityTracker() throws BacklogReportingException, InitializationException {
        putTestFile("entities/first.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("entities/second.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final S3Client spyClient = spy(getClient());
        final TestRunner runner = initEntityTrackingBacklogRunner(new TestableListS3(spyClient));
        runner.setProperty(ListS3.LISTING_STRATEGY, ListS3.BY_ENTITIES.getValue());
        runner.setProperty(ListS3.PREFIX, "entities/");
        runner.run(0, false, true);

        final Backlog firstRunBacklog = reportBacklog(runner).orElseThrow();
        assertEquals(Backlog.Precision.EXACT, firstRunBacklog.getPrecision());
        assertTrue(firstRunBacklog.getFlowFileCount().isPresent());
        assertEquals(2L, firstRunBacklog.getFlowFileCount().getAsLong());
        assertFalse(firstRunBacklog.getLastCaughtUp().isPresent());

        runner.run(1, false, false);

        putTestFile("entities/third.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final Backlog backlog = reportBacklog(runner).orElseThrow();
        assertEquals(Backlog.Precision.EXACT, backlog.getPrecision());
        assertTrue(backlog.getFlowFileCount().isPresent());
        assertEquals(1L, backlog.getFlowFileCount().getAsLong());
        assertFalse(backlog.getLastCaughtUp().isPresent());
        verify(spyClient, atLeastOnce()).listObjectsV2(any(ListObjectsV2Request.class));

        runner.run(1, false, false);

        final Backlog caughtUpBacklog = reportBacklog(runner).orElseThrow();
        assertEquals(Backlog.Precision.EXACT, caughtUpBacklog.getPrecision());
        assertTrue(caughtUpBacklog.getFlowFileCount().isPresent());
        assertEquals(0L, caughtUpBacklog.getFlowFileCount().getAsLong());
        assertTrue(caughtUpBacklog.getLastCaughtUp().isPresent());
    }

    @Test
    public void testBacklogWrapsListObjectsV2Errors() throws InitializationException {
        final S3Client spyClient = spy(getClient());
        doThrow(S3Exception.builder().message("boom").build()).when(spyClient).listObjectsV2(any(ListObjectsV2Request.class));

        final TestRunner runner = initBacklogRunner(new TestableListS3(spyClient));
        runner.setProperty(ListS3.LISTING_STRATEGY, ListS3.BY_TIMESTAMPS.getValue());
        runner.run(0, false, true);

        final BacklogReportingException exception = assertThrows(BacklogReportingException.class, () -> reportBacklog(runner));
        assertTrue(exception.getMessage().contains("Failed to list S3 bucket"));
        assertTrue(exception.getMessage().contains("while reporting backlog"));
        assertTrue(exception.getMessage().contains(BUCKET_NAME));
        assertInstanceOf(S3Exception.class, exception.getCause());
    }

    @Test
    public void testBacklogWhileStoppedWithLagBeforeOnScheduled() throws BacklogReportingException, InitializationException {
        putTestFile("stopped-lag/first.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("stopped-lag/second.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final S3Client spyClient = spy(getClient());
        final TestRunner runner = initBacklogRunner(new TestableListS3(spyClient));
        runner.setProperty(ListS3.LISTING_STRATEGY, ListS3.BY_TIMESTAMPS.getValue());
        runner.setProperty(ListS3.PREFIX, "stopped-lag/");

        // Never call runner.run(), so @OnScheduled is never invoked. The fresh-query path must still answer.
        // Because no listing state has been recorded yet, every object that satisfies the listing filters is
        // unread, so the count is reported as EXACT.
        final Backlog backlog = reportBacklog(runner).orElseThrow();
        assertEquals(Backlog.Precision.EXACT, backlog.getPrecision());
        assertTrue(backlog.getFlowFileCount().isPresent());
        assertEquals(2L, backlog.getFlowFileCount().getAsLong());
        assertFalse(backlog.getLastCaughtUp().isPresent());
        verify(spyClient, atLeastOnce()).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    public void testBacklogWhileStoppedAfterCaughtUp() throws BacklogReportingException, InitializationException {
        putTestFile("stopped-caught-up/first.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("stopped-caught-up/second.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final S3Client spyClient = spy(getClient());
        final TestRunner runner = initBacklogRunner(new TestableListS3(spyClient));
        runner.setProperty(ListS3.LISTING_STRATEGY, ListS3.BY_TIMESTAMPS.getValue());
        runner.setProperty(ListS3.PREFIX, "stopped-caught-up/");

        // Drain the listing while running, then stop the processor; the subsequent backlog call
        // happens with no @OnScheduled context active and must still produce a meaningful answer.
        runner.run(2, true, true);

        final Backlog backlog = reportBacklog(runner).orElseThrow();
        assertEquals(Backlog.Precision.EXACT, backlog.getPrecision());
        assertTrue(backlog.getFlowFileCount().isPresent());
        assertEquals(0L, backlog.getFlowFileCount().getAsLong());
        assertTrue(backlog.getLastCaughtUp().isPresent());
    }

    @Test
    public void testEntityTrackingBacklogWithVersionsEnabledIdentifiesEntitiesByVersionId() throws BacklogReportingException, InitializationException {
        putTestFile("versioned/first.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final S3Client spyClient = spy(getClient());
        final TestRunner runner = initEntityTrackingBacklogRunner(new TestableListS3(spyClient));
        runner.setProperty(ListS3.LISTING_STRATEGY, ListS3.BY_ENTITIES.getValue());
        runner.setProperty(ListS3.PREFIX, "versioned/");
        runner.setProperty(ListS3.USE_VERSIONS, "true");
        runner.run(1, false, true);

        final Backlog firstRunBacklog = reportBacklog(runner).orElseThrow();
        assertEquals(Backlog.Precision.EXACT, firstRunBacklog.getPrecision());
        assertTrue(firstRunBacklog.getFlowFileCount().isPresent());
        assertEquals(0L, firstRunBacklog.getFlowFileCount().getAsLong());

        putTestFile("versioned/second.txt", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final Backlog backlog = reportBacklog(runner).orElseThrow();
        assertEquals(Backlog.Precision.EXACT, backlog.getPrecision());
        assertTrue(backlog.getFlowFileCount().isPresent());
        assertEquals(1L, backlog.getFlowFileCount().getAsLong());
        verify(spyClient, never()).headObject(any(HeadObjectRequest.class));
    }

    private TestRunner initBacklogRunner(final ListS3 processor) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        setSecureProperties(runner);
        runner.setProperty(RegionUtil.REGION, getRegion());
        runner.setProperty(ListS3.ENDPOINT_OVERRIDE, getEndpointOverride());
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        // These backlog tests assert against the List Objects V2 endpoint, so List Type must be set
        // explicitly since the Processor's default is List Objects V1.
        runner.setProperty(ListS3.LIST_TYPE, "2");
        return runner;
    }

    private TestRunner initEntityTrackingBacklogRunner(final ListS3 processor) throws InitializationException {
        final TestRunner runner = initBacklogRunner(processor);
        final EphemeralMapCacheClientService trackingStateCache = new EphemeralMapCacheClientService();
        runner.addControllerService("tracking-state-cache", trackingStateCache);
        runner.enableControllerService(trackingStateCache);
        runner.setProperty(ListS3.TRACKING_STATE_CACHE, "tracking-state-cache");
        return runner;
    }

    private Optional<Backlog> reportBacklog(final TestRunner runner) throws BacklogReportingException {
        final BacklogReportingProcessor backlogReportingProcessor = (BacklogReportingProcessor) runner.getProcessor();
        return backlogReportingProcessor.getBacklog(runner.getProcessContext());
    }

    private static final class TestableListS3 extends ListS3 {
        private final S3Client client;

        private TestableListS3(final S3Client client) {
            this.client = client;
        }

        @Override
        protected S3Client getClient(final ProcessContext context) {
            return client;
        }

        @Override
        protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
            return client;
        }

        @Override
        protected S3Client createClient(final ProcessContext context, final Map<String, String> attributes) {
            return client;
        }
    }

    /**
     * Minimal in-memory {@link DistributedMapCacheClient} backing the {@link ListS3} entity-tracker for the
     * tests in this class. Only the cache methods actually exercised by
     * {@link org.apache.nifi.processor.util.list.ListedEntityTracker} ({@code put}, {@code get}, and
     * {@code remove}) are implemented; every other method on the interface throws to make accidental usage
     * obvious during test development.
     */
    private static final class EphemeralMapCacheClientService extends AbstractControllerService implements DistributedMapCacheClient {
        private final Map<ByteBuffer, byte[]> stored = new HashMap<>();

        @Override
        public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
            stored.put(serializeAsByteBuffer(key, keySerializer), serializeAsBytes(value, valueSerializer));
        }

        @Override
        public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
            final byte[] storedValue = stored.get(serializeAsByteBuffer(key, keySerializer));
            if (storedValue == null) {
                return null;
            }
            return valueDeserializer.deserialize(storedValue);
        }

        @Override
        public <K> boolean remove(final K key, final Serializer<K> serializer) {
            return stored.remove(serializeAsByteBuffer(key, serializer)) != null;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
            throw new UnsupportedOperationException("putIfAbsent is not exercised by these tests");
        }

        @Override
        public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer) {
            throw new UnsupportedOperationException("getAndPutIfAbsent is not exercised by these tests");
        }

        @Override
        public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) {
            throw new UnsupportedOperationException("containsKey is not exercised by these tests");
        }

        private static <T> ByteBuffer serializeAsByteBuffer(final T value, final Serializer<T> serializer) {
            return ByteBuffer.wrap(serializeAsBytes(value, serializer));
        }

        private static <T> byte[] serializeAsBytes(final T value, final Serializer<T> serializer) {
            try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                serializer.serialize(value, outputStream);
                return outputStream.toByteArray();
            } catch (final IOException exception) {
                throw new UncheckedIOException("Failed to serialize cache value", exception);
            }
        }
    }
}
