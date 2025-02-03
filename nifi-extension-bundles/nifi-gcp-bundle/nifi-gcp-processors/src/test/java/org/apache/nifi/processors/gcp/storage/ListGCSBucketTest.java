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
package org.apache.nifi.processors.gcp.storage;

import com.google.api.gax.paging.Page;
import com.google.cloud.PageImpl;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ACL_OWNER_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ACL_READER_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ACL_WRITER_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.BUCKET_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CACHE_CONTROL_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.COMPONENT_COUNT_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CONTENT_DISPOSITION_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CONTENT_ENCODING_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CONTENT_LANGUAGE_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CRC32C_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CREATE_TIME_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ENCRYPTION_ALGORITHM_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ENCRYPTION_SHA256_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ETAG_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.GENERATED_ID_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.GENERATION_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.KEY_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.MD5_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.MEDIA_LINK_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.METAGENERATION_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.OWNER_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.OWNER_TYPE_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.UPDATE_TIME_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.URI_ATTR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ListGCSBucket} which do not consume Google Cloud resources.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
public class ListGCSBucketTest extends AbstractGCSTest {

    private static final String PREFIX = "test-prefix";
    private static final Boolean USE_GENERATIONS = true;

    private static final Long SIZE = 100L;
    private static final String CACHE_CONTROL = "test-cache-control";
    private static final Integer COMPONENT_COUNT = 3;
    private static final String CONTENT_ENCODING = "test-content-encoding";
    private static final String CONTENT_LANGUAGE = "test-content-language";
    private static final String CONTENT_TYPE = "test-content-type";
    private static final String CRC32C = "test-crc32c";
    private static final String ENCRYPTION = "test-encryption";
    private static final String ENCRYPTION_SHA256 = "test-encryption-256";
    private static final String ETAG = "test-etag";
    private static final String GENERATED_ID = "test-generated-id";
    private static final String MD5 = "test-md5";
    private static final String MEDIA_LINK = "test-media-link";
    private static final Long METAGENERATION = 42L;
    private static final String OWNER_USER_EMAIL = "test-owner-user-email";
    private static final String OWNER_GROUP_EMAIL = "test-owner-group-email";
    private static final String OWNER_DOMAIN = "test-owner-domain";
    private static final String OWNER_PROJECT_ID = "test-owner-project-id";
    private static final String URI = "test-uri";
    private static final String CONTENT_DISPOSITION = "attachment; filename=\"test-content-disposition.txt\"";
    private static final Long CREATE_TIME = 1234L;
    private static final Long UPDATE_TIME = 4567L;
    private final static Long GENERATION = 5L;
    private static final long TIMESTAMP = 1234567890;

    @Mock
    Storage storage;

    @Mock
    Page<Blob> mockBlobPage;

    @Captor
    ArgumentCaptor<Storage.BlobListOption> argumentCaptor;

    private TestRunner runner;

    private ListGCSBucket processor;

    private MockStateManager mockStateManager;

    @Mock
    private DistributedMapCacheClient mockCache;

    @BeforeEach
    public void beforeEach() throws Exception {
        processor = new ListGCSBucket() {
            @Override
            protected Storage getCloudService() {
                return storage;
            }

            @Override
            protected Storage getCloudService(final ProcessContext context) {
                return storage;
            }
        };

        runner = buildNewRunner(processor);
        runner.setProperty(ListGCSBucket.BUCKET, BUCKET);
        runner.assertValid();

        mockStateManager = runner.getStateManager();
    }

    @Override
    public ListGCSBucket getProcessor() {
        return processor;
    }

    @Override
    protected void addRequiredPropertiesToRunner(TestRunner runner) {
    }

    @Test
    public void testRestoreFreshState() throws Exception {
        assertFalse(runner.getProcessContext().getStateManager().getState(Scope.CLUSTER).getStateVersion().isPresent(), "Cluster StateMap should be fresh (version -1L)");
        assertTrue(processor.getStateKeys().isEmpty());

        processor.restoreState(runner.getProcessSessionFactory().createSession());

        assertTrue(processor.getStateKeys().isEmpty());
        assertEquals(0L, processor.getStateTimestamp());

        assertTrue(processor.getStateKeys().isEmpty());
    }

    @Test
    public void testRestorePreviousState() throws Exception {
        final Map<String, String> state = new LinkedHashMap<>();
        state.put(ListGCSBucket.CURRENT_TIMESTAMP, String.valueOf(4L));
        state.put(ListGCSBucket.CURRENT_KEY_PREFIX + "0", "test-key-0");
        state.put(ListGCSBucket.CURRENT_KEY_PREFIX + "1", "test-key-1");

        runner.getStateManager().setState(state, Scope.CLUSTER);

        assertTrue(processor.getStateKeys().isEmpty());
        assertEquals(0L, processor.getStateTimestamp());

        processor.restoreState(runner.getProcessSessionFactory().createSession());

        assertNotNull(processor.getStateKeys());
        assertTrue(processor.getStateKeys().contains("test-key-0"));
        assertTrue(processor.getStateKeys().contains("test-key-1"));
        assertEquals(4L, processor.getStateTimestamp());
    }

    @Test
    public void testPersistState() throws Exception {
        assertFalse(
                runner.getProcessContext().getStateManager().getState(Scope.CLUSTER).getStateVersion().isPresent(),
                "Cluster StateMap should be fresh"
        );

        final Set<String> keys = new LinkedHashSet<>(Arrays.asList("test-key-0", "test-key-1"));
        final ProcessSession session = runner.getProcessSessionFactory().createSession();
        processor.persistState(session, 4L, keys);

        final StateMap stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        assertTrue(stateMap.getStateVersion().isPresent(), "Cluster StateMap should have been written to");

        final Map<String, String> state = new HashMap<>();
        state.put(ListGCSBucket.CURRENT_TIMESTAMP, String.valueOf(4L));
        state.put(ListGCSBucket.CURRENT_KEY_PREFIX + "0", "test-key-0");
        state.put(ListGCSBucket.CURRENT_KEY_PREFIX + "1", "test-key-1");

        assertEquals(state, stateMap.toMap());
    }

    @Test
    public void testFailedPersistState() {
        runner.getStateManager().setFailOnStateSet(Scope.CLUSTER, true);

        final Set<String> keys = new HashSet<>(Arrays.asList("test-key-0", "test-key-1"));

        assertTrue(runner.getLogger().getErrorMessages().isEmpty());

        final ProcessSession session = runner.getProcessSessionFactory().createSession();
        processor.persistState(session, 4L, keys);

        // The method should have caught the error and reported it to the logger.
        final List<LogMessage> logMessages = runner.getLogger().getErrorMessages();
        assertFalse(logMessages.isEmpty());
        assertEquals(
                1,
                logMessages.size()
        );

        // We could do more specific things like check the contents of the LogMessage,
        // but that seems too nitpicky.
    }

    @Test
    public void testSuccessfulList() {
        final Iterable<Blob> mockList = Arrays.asList(
                buildMockBlob("blob-bucket-1", "blob-key-1", 2L),
                buildMockBlob("blob-bucket-2", "blob-key-2", 3L)
        );

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        when(storage.testIamPermissions(anyString(), any())).thenReturn(Collections.singletonList(true));

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS);
        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 2);
        verifyConfigVerification(runner, processor, 2);

        final List<MockFlowFile> successes = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);

        MockFlowFile flowFile = successes.get(0);
        assertEquals("blob-bucket-1", flowFile.getAttribute(BUCKET_ATTR));
        assertEquals("blob-key-1", flowFile.getAttribute(KEY_ATTR));
        assertEquals("2", flowFile.getAttribute(UPDATE_TIME_ATTR));

        flowFile = successes.get(1);
        assertEquals("blob-bucket-2", flowFile.getAttribute(BUCKET_ATTR));
        assertEquals("blob-key-2", flowFile.getAttribute(KEY_ATTR));
        assertEquals("3", flowFile.getAttribute(UPDATE_TIME_ATTR));

        assertEquals(3L, processor.getStateTimestamp());

        assertEquals(Collections.singleton("blob-key-2"), processor.getStateKeys());
    }

    @Test
    public void testNoTrackingListing() {
        runner.setProperty(ListGCSBucket.LISTING_STRATEGY, ListGCSBucket.NO_TRACKING);
        runner.assertValid();

        final Iterable<Blob> mockList = Arrays.asList(
                buildMockBlob("blob-bucket-1", "blob-key-1", 2L),
                buildMockBlob("blob-bucket-2", "blob-key-2", 3L)
        );

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        runner.run();

        when(storage.testIamPermissions(anyString(), any())).thenReturn(Collections.singletonList(true));

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS);
        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 2);
        verifyConfigVerification(runner, processor, 2);

        final List<MockFlowFile> successes = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);

        MockFlowFile flowFile = successes.get(0);
        assertEquals("blob-bucket-1", flowFile.getAttribute(BUCKET_ATTR));
        assertEquals("blob-key-1", flowFile.getAttribute(KEY_ATTR));
        assertEquals("2", flowFile.getAttribute(UPDATE_TIME_ATTR));

        flowFile = successes.get(1);
        assertEquals("blob-bucket-2", flowFile.getAttribute(BUCKET_ATTR));
        assertEquals("blob-key-2", flowFile.getAttribute(KEY_ATTR));
        assertEquals("3", flowFile.getAttribute(UPDATE_TIME_ATTR));

        runner.clearTransferState();

        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS);
        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 2);

        assertEquals(0, processor.getStateTimestamp());
        assertEquals(0, processor.getStateKeys().size());
    }

    @Test
    public void testOldValues() throws Exception {
        final Iterable<Blob> mockList = Collections.singletonList(
                buildMockBlob("blob-bucket-1", "blob-key-1", 2L)
        );

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.enqueue("test2");
        runner.run(2);

        when(storage.testIamPermissions(anyString(), any())).thenReturn(Collections.singletonList(true));

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS);
        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 1);
        verifyConfigVerification(runner, processor, 1);

        assertEquals("blob-key-1", runner.getStateManager().getState(Scope.CLUSTER).get(ListGCSBucket.CURRENT_KEY_PREFIX + "0"));
        assertEquals("2", runner.getStateManager().getState(Scope.CLUSTER).get(ListGCSBucket.CURRENT_TIMESTAMP));
    }

    @Test
    public void testEmptyList() throws Exception {
        final Iterable<Blob> mockList = Collections.emptyList();

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        when(storage.testIamPermissions(anyString(), any())).thenReturn(Collections.singletonList(true));

        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 0);
        verifyConfigVerification(runner, processor, 0);

        assertFalse(runner.getStateManager().getState(Scope.CLUSTER).getStateVersion().isPresent(), "No state should be persisted on an empty return");
    }

    @Test
    public void testListWithStateAndFilesComingInAlphabeticalOrder() throws Exception {
        final Map<String, String> state = new LinkedHashMap<>();
        state.put(ListGCSBucket.CURRENT_TIMESTAMP, String.valueOf(1L));
        state.put(ListGCSBucket.CURRENT_KEY_PREFIX + "0", "blob-key-1");

        runner.getStateManager().setState(state, Scope.CLUSTER);

        final Iterable<Blob> mockList = Arrays.asList(
                buildMockBlobWithoutBucket("blob-bucket-1", "blob-key-1", 1L),
                buildMockBlob("blob-bucket-2", "blob-key-2", 2L)
        );

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        when(storage.testIamPermissions(anyString(), any())).thenReturn(Collections.singletonList(true));

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS);
        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 1);

        // Both blobs are counted, because verification does not account for entity tracking
        verifyConfigVerification(runner, processor, 2);

        final List<MockFlowFile> successes = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);

        MockFlowFile flowFile = successes.get(0);
        assertEquals("blob-bucket-2", flowFile.getAttribute(BUCKET_ATTR));
        assertEquals("blob-key-2", flowFile.getAttribute(KEY_ATTR));
        assertEquals("2", flowFile.getAttribute(UPDATE_TIME_ATTR));
        assertEquals(2L, processor.getStateTimestamp());
        assertEquals(Collections.singleton("blob-key-2"), processor.getStateKeys());
    }

    @Test
    public void testListWithStateAndFilesComingNotInAlphabeticalOrder() throws Exception {
        final Map<String, String> state = new LinkedHashMap<>();
        state.put(ListGCSBucket.CURRENT_TIMESTAMP, String.valueOf(1L));
        state.put(ListGCSBucket.CURRENT_KEY_PREFIX + "0", "blob-key-2");

        runner.getStateManager().setState(state, Scope.CLUSTER);

        final Iterable<Blob> mockList = Arrays.asList(
                buildMockBlob("blob-bucket-1", "blob-key-1", 2L),
                buildMockBlobWithoutBucket("blob-bucket-2", "blob-key-2", 1L)
        );

        when(mockBlobPage.getValues())
                .thenReturn(mockList);

        when(mockBlobPage.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        when(storage.testIamPermissions(anyString(), any())).thenReturn(Collections.singletonList(true));

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS);
        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 1);

        // Both blobs are counted, because verification does not account for entity tracking
        verifyConfigVerification(runner, processor, 2);

        final List<MockFlowFile> successes = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);

        MockFlowFile flowFile = successes.get(0);
        assertEquals("blob-bucket-1", flowFile.getAttribute(BUCKET_ATTR));
        assertEquals("blob-key-1", flowFile.getAttribute(KEY_ATTR));
        assertEquals("2", flowFile.getAttribute(UPDATE_TIME_ATTR));
        assertEquals(2L, processor.getStateTimestamp());

        assertEquals(Collections.singleton("blob-key-1"), processor.getStateKeys());
    }

    @Test
    public void testListWithStateAndNewFilesComingWithTheSameTimestamp() throws Exception {
        final Map<String, String> state = new LinkedHashMap<>();
        state.put(ListGCSBucket.CURRENT_TIMESTAMP, String.valueOf(1L));
        state.put(ListGCSBucket.CURRENT_KEY_PREFIX + "0", "blob-key-2");

        runner.getStateManager().setState(state, Scope.CLUSTER);

        final Iterable<Blob> mockList = Arrays.asList(
                buildMockBlob("blob-bucket-1", "blob-key-1", 2L),
                buildMockBlobWithoutBucket("blob-bucket-2", "blob-key-2", 1L),
                buildMockBlob("blob-bucket-3", "blob-key-3", 2L)
        );

        when(mockBlobPage.getValues())
                .thenReturn(mockList);

        when(mockBlobPage.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPage);

        when(storage.testIamPermissions(anyString(), any())).thenReturn(Collections.singletonList(true));

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS);
        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 2);

        // All blobs are counted, because verification does not account for entity tracking
        verifyConfigVerification(runner, processor, 3);

        final List<MockFlowFile> successes = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);

        MockFlowFile flowFile = successes.get(0);
        assertEquals("blob-bucket-1", flowFile.getAttribute(BUCKET_ATTR));
        assertEquals("blob-key-1", flowFile.getAttribute(KEY_ATTR));
        assertEquals("2", flowFile.getAttribute(UPDATE_TIME_ATTR));

        flowFile = successes.get(1);
        assertEquals("blob-bucket-3", flowFile.getAttribute(BUCKET_ATTR));
        assertEquals("blob-key-3", flowFile.getAttribute(KEY_ATTR));
        assertEquals("2", flowFile.getAttribute(UPDATE_TIME_ATTR));
        assertEquals(2L, processor.getStateTimestamp());

        assertEquals(new HashSet<>(Arrays.asList("blob-key-1", "blob-key-3")), processor.getStateKeys());
    }

    @Test
    public void testListWithStateAndNewFilesComingWithTheCurrentTimestamp() throws Exception {
        final Map<String, String> state = new LinkedHashMap<>();
        state.put(ListGCSBucket.CURRENT_TIMESTAMP, String.valueOf(1L));
        state.put(ListGCSBucket.CURRENT_KEY_PREFIX + "0", "blob-key-2");

        runner.getStateManager().setState(state, Scope.CLUSTER);

        final Iterable<Blob> mockList = Arrays.asList(
                buildMockBlob("blob-bucket-1", "blob-key-1", 1L),
                buildMockBlobWithoutBucket("blob-bucket-2", "blob-key-2", 1L),
                buildMockBlob("blob-bucket-3", "blob-key-3", 1L)
        );

        when(mockBlobPage.getValues())
                .thenReturn(mockList);

        when(mockBlobPage.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPage);

        when(storage.testIamPermissions(anyString(), any())).thenReturn(Collections.singletonList(true));

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS);
        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 2);

        // All blobs are counted, because verification does not account for entity tracking
        verifyConfigVerification(runner, processor, 3);

        final List<MockFlowFile> successes = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);

        MockFlowFile flowFile = successes.get(0);
        assertEquals("blob-bucket-1", flowFile.getAttribute(BUCKET_ATTR));
        assertEquals("blob-key-1", flowFile.getAttribute(KEY_ATTR));
        assertEquals("1", flowFile.getAttribute(UPDATE_TIME_ATTR));

        flowFile = successes.get(1);
        assertEquals("blob-bucket-3", flowFile.getAttribute(BUCKET_ATTR));
        assertEquals("blob-key-3", flowFile.getAttribute(KEY_ATTR));
        assertEquals("1", flowFile.getAttribute(UPDATE_TIME_ATTR));
        assertEquals(1L, processor.getStateTimestamp());
        assertEquals(new HashSet<>(Arrays.asList("blob-key-1", "blob-key-3")), processor.getStateKeys());
    }

    @Test
    public void testAttributesSet() {
        final Blob blob = buildMockBlob("test-bucket-1", "test-key-1", 2L);
        when(blob.getSize()).thenReturn(SIZE);
        when(blob.getCacheControl()).thenReturn(CACHE_CONTROL);
        when(blob.getComponentCount()).thenReturn(COMPONENT_COUNT);
        when(blob.getContentEncoding()).thenReturn(CONTENT_ENCODING);
        when(blob.getContentLanguage()).thenReturn(CONTENT_LANGUAGE);
        when(blob.getContentType()).thenReturn(CONTENT_TYPE);
        when(blob.getCrc32c()).thenReturn(CRC32C);

        final BlobInfo.CustomerEncryption mockEncryption = mock(BlobInfo.CustomerEncryption.class);
        when(mockEncryption.getEncryptionAlgorithm()).thenReturn(ENCRYPTION);
        when(mockEncryption.getKeySha256()).thenReturn(ENCRYPTION_SHA256);
        when(blob.getCustomerEncryption()).thenReturn(mockEncryption);

        when(blob.getEtag()).thenReturn(ETAG);
        when(blob.getGeneratedId()).thenReturn(GENERATED_ID);
        when(blob.getGeneration()).thenReturn(GENERATION);
        when(blob.getMd5()).thenReturn(MD5);
        when(blob.getMediaLink()).thenReturn(MEDIA_LINK);
        when(blob.getMetageneration()).thenReturn(METAGENERATION);
        when(blob.getSelfLink()).thenReturn(URI);
        when(blob.getContentDisposition()).thenReturn(CONTENT_DISPOSITION);
        when(blob.getCreateTimeOffsetDateTime()).thenReturn(offsetDateTime(CREATE_TIME));
        when(blob.getUpdateTimeOffsetDateTime()).thenReturn(offsetDateTime(UPDATE_TIME));

        final Iterable<Blob> mockList = Collections.singletonList(blob);

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(CACHE_CONTROL, flowFile.getAttribute(CACHE_CONTROL_ATTR));

        assertEquals(COMPONENT_COUNT, Integer.valueOf(flowFile.getAttribute(COMPONENT_COUNT_ATTR)));
        assertEquals(CONTENT_ENCODING, flowFile.getAttribute(CONTENT_ENCODING_ATTR));
        assertEquals(CONTENT_LANGUAGE, flowFile.getAttribute(CONTENT_LANGUAGE_ATTR));
        assertEquals(CONTENT_TYPE, flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));
        assertEquals(CRC32C, flowFile.getAttribute(CRC32C_ATTR));
        assertEquals(ENCRYPTION, flowFile.getAttribute(ENCRYPTION_ALGORITHM_ATTR));
        assertEquals(ENCRYPTION_SHA256, flowFile.getAttribute(ENCRYPTION_SHA256_ATTR));
        assertEquals(ETAG, flowFile.getAttribute(ETAG_ATTR));
        assertEquals(GENERATED_ID, flowFile.getAttribute(GENERATED_ID_ATTR));
        assertEquals(GENERATION, Long.valueOf(flowFile.getAttribute(GENERATION_ATTR)));
        assertEquals(MD5, flowFile.getAttribute(MD5_ATTR));
        assertEquals(MEDIA_LINK, flowFile.getAttribute(MEDIA_LINK_ATTR));
        assertEquals(METAGENERATION, Long.valueOf(flowFile.getAttribute(METAGENERATION_ATTR)));
        assertEquals(URI, flowFile.getAttribute(URI_ATTR));
        assertEquals(CONTENT_DISPOSITION, flowFile.getAttribute(CONTENT_DISPOSITION_ATTR));
        assertEquals(CREATE_TIME, Long.valueOf(flowFile.getAttribute(CREATE_TIME_ATTR)));
        assertEquals(UPDATE_TIME, Long.valueOf(flowFile.getAttribute(UPDATE_TIME_ATTR)));
    }

    @Test
    public void testAclOwnerUser() {
        final Blob blob = buildMockBlob("test-bucket-1", "test-key-1", 2L);
        final Acl.User mockUser = mock(Acl.User.class);
        when(mockUser.getEmail()).thenReturn(OWNER_USER_EMAIL);
        when(mockUser.getType()).thenReturn(Acl.Entity.Type.USER);
        when(blob.getOwner()).thenReturn(mockUser);

        final Iterable<Blob> mockList = Collections.singletonList(blob);

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(OWNER_USER_EMAIL, flowFile.getAttribute(OWNER_ATTR));
        assertEquals("user", flowFile.getAttribute(OWNER_TYPE_ATTR));
    }

    @Test
    public void testAclOwnerGroup() {
        final Blob blob = buildMockBlob("test-bucket-1", "test-key-1", 2L);
        final Acl.Group mockGroup = mock(Acl.Group.class);
        when(mockGroup.getEmail()).thenReturn(OWNER_GROUP_EMAIL);
        when(mockGroup.getType()).thenReturn(Acl.Entity.Type.GROUP);
        when(blob.getOwner()).thenReturn(mockGroup);

        final Iterable<Blob> mockList = Collections.singletonList(blob);

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(OWNER_GROUP_EMAIL, flowFile.getAttribute(OWNER_ATTR));
        assertEquals("group", flowFile.getAttribute(OWNER_TYPE_ATTR));
    }

    @Test
    public void testAclOwnerDomain() {
        final Blob blob = buildMockBlob("test-bucket-1", "test-key-1", 2L);
        final Acl.Domain mockDomain = mock(Acl.Domain.class);
        when(mockDomain.getDomain()).thenReturn(OWNER_DOMAIN);
        when(mockDomain.getType()).thenReturn(Acl.Entity.Type.DOMAIN);
        when(blob.getOwner()).thenReturn(mockDomain);

        final Iterable<Blob> mockList = Collections.singletonList(blob);
        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(OWNER_DOMAIN, flowFile.getAttribute(OWNER_ATTR));
        assertEquals("domain", flowFile.getAttribute(OWNER_TYPE_ATTR));
    }

    @Test
    public void testAclOwnerProject() {
        final Blob blob = buildMockBlob("test-bucket-1", "test-key-1", 2L);
        final Acl.Project mockProject = mock(Acl.Project.class);
        when(mockProject.getProjectId()).thenReturn(OWNER_PROJECT_ID);
        when(mockProject.getType()).thenReturn(Acl.Entity.Type.PROJECT);
        when(blob.getOwner()).thenReturn(mockProject);

        final Iterable<Blob> mockList = Collections.singletonList(blob);

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(OWNER_PROJECT_ID, flowFile.getAttribute(OWNER_ATTR));
        assertEquals("project", flowFile.getAttribute(OWNER_TYPE_ATTR));
    }

    @Test
    public void testFileAcls() {
        final Blob blob = buildMockBlob("test-bucket-1", "test-key-1", 2L);
        final Acl mockUser = mock(Acl.class);
        final Acl.User mockUserEntity = mock(Acl.User.class);
        final Acl mockGroup = mock(Acl.class);
        final Acl.Group mockGroupEntity = mock(Acl.Group.class);
        final Acl mockDomain = mock(Acl.class);
        final Acl.Domain mockDomainEntity = mock(Acl.Domain.class);
        final Acl mockProject = mock(Acl.class);
        final Acl.Project mockProjectEntity = mock(Acl.Project.class);

        when(blob.getAcl()).thenReturn(List.of(mockUser, mockGroup, mockDomain, mockProject));

        when(mockUser.getEntity()).thenReturn(mockUserEntity);
        when(mockUser.getRole()).thenReturn(Acl.Role.OWNER);
        when(mockUserEntity.getType()).thenReturn(Acl.Entity.Type.USER);
        when(mockUserEntity.getEmail()).thenReturn(OWNER_USER_EMAIL);

        when(mockGroup.getEntity()).thenReturn(mockGroupEntity);
        when(mockGroup.getRole()).thenReturn(Acl.Role.WRITER);
        when(mockGroupEntity.getType()).thenReturn(Acl.Entity.Type.GROUP);
        when(mockGroupEntity.getEmail()).thenReturn(OWNER_GROUP_EMAIL);

        when(mockDomain.getEntity()).thenReturn(mockDomainEntity);
        when(mockDomain.getRole()).thenReturn(Acl.Role.WRITER);
        when(mockDomainEntity.getType()).thenReturn(Acl.Entity.Type.DOMAIN);
        when(mockDomainEntity.getDomain()).thenReturn(OWNER_DOMAIN);

        when(mockProject.getEntity()).thenReturn(mockProjectEntity);
        when(mockProject.getRole()).thenReturn(Acl.Role.READER);
        when(mockProjectEntity.getType()).thenReturn(Acl.Entity.Type.PROJECT);
        when(mockProjectEntity.getProjectId()).thenReturn(OWNER_PROJECT_ID);

        final Iterable<Blob> mockList = Collections.singletonList(blob);

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(OWNER_USER_EMAIL, flowFile.getAttribute(ACL_OWNER_ATTR));
        assertEquals("%s,%s".formatted(OWNER_GROUP_EMAIL, OWNER_DOMAIN), flowFile.getAttribute(ACL_WRITER_ATTR));
        assertEquals(OWNER_PROJECT_ID, flowFile.getAttribute(ACL_READER_ATTR));
    }

    @Test
    public void testYieldOnBadStateRestore() {

        runner.getStateManager().setFailOnStateGet(Scope.CLUSTER, true);
        runner.enqueue("test");
        runner.run();

        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 0);
        assertEquals(1, runner.getLogger().getErrorMessages().size());
    }

    @Test
    public void testListOptionsPrefix() {
        runner.setProperty(ListGCSBucket.PREFIX, PREFIX);
        runner.assertValid();

        final Iterable<Blob> mockList = Collections.emptyList();

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), argumentCaptor.capture())).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        assertEquals(Storage.BlobListOption.prefix(PREFIX), argumentCaptor.getValue());
    }

    @Test
    public void testListOptionsVersions() {
        runner.setProperty(ListGCSBucket.USE_GENERATIONS, String.valueOf(USE_GENERATIONS));
        runner.assertValid();

        final Iterable<Blob> mockList = Collections.emptyList();

        when(mockBlobPage.getValues()).thenReturn(mockList);
        when(mockBlobPage.getNextPage()).thenReturn(null);
        when(storage.list(anyString(), argumentCaptor.capture())).thenReturn(mockBlobPage);

        runner.enqueue("test");
        runner.run();

        Storage.BlobListOption option = argumentCaptor.getValue();
        assertEquals(Storage.BlobListOption.versions(true), option);
    }

    @Test
    void testResetTimestampTrackingWhenBucketModified() throws Exception {
        setUpResetTrackingTest(ListGCSBucket.BY_TIMESTAMPS);

        assertFalse(processor.isResetTracking());

        runner.run();

        assertEquals(TIMESTAMP, processor.getCurrentTimestamp());

        runner.setProperty(ListGCSBucket.BUCKET, "otherBucket");

        assertTrue(processor.isResetTracking());

        runner.run();

        assertEquals(0, processor.getCurrentTimestamp());
        mockStateManager.assertStateNotSet(ListGCSBucket.CURRENT_TIMESTAMP, Scope.CLUSTER);

        assertFalse(processor.isResetTracking());
    }

    @Test
    void testResetTimestampTrackingWhenPrefixModified() throws Exception {
        setUpResetTrackingTest(ListGCSBucket.BY_TIMESTAMPS);

        assertFalse(processor.isResetTracking());

        runner.run();

        assertEquals(TIMESTAMP, processor.getCurrentTimestamp());

        runner.setProperty(ListGCSBucket.PREFIX, "prefix2");

        assertTrue(processor.isResetTracking());

        runner.run();

        assertEquals(0, processor.getCurrentTimestamp());
        mockStateManager.assertStateNotSet(ListGCSBucket.CURRENT_TIMESTAMP, Scope.CLUSTER);

        assertFalse(processor.isResetTracking());
    }

    @Test
    void testResetTimestampTrackingWhenStrategyModified() throws Exception {
        setUpResetTrackingTest(ListGCSBucket.BY_TIMESTAMPS);

        assertFalse(processor.isResetTracking());

        runner.run();

        assertEquals(TIMESTAMP, processor.getCurrentTimestamp());

        runner.setProperty(ListGCSBucket.LISTING_STRATEGY, ListGCSBucket.NO_TRACKING);

        assertTrue(processor.isResetTracking());

        runner.run();

        assertEquals(0, processor.getCurrentTimestamp());
        mockStateManager.assertStateNotSet(ListGCSBucket.CURRENT_TIMESTAMP, Scope.CLUSTER);

        assertFalse(processor.isResetTracking());
    }

    @Test
    void testResetEntityTrackingWhenBucketModified() throws Exception {
        setUpResetTrackingTest(ListGCSBucket.BY_ENTITIES);

        assertFalse(processor.isResetTracking());

        runner.run();

        assertNotNull(processor.getListedEntityTracker());

        runner.setProperty(ListGCSBucket.BUCKET, "otherBucket");

        assertTrue(processor.isResetTracking());

        runner.run();

        assertNotNull(processor.getListedEntityTracker());
        verify(mockCache).remove(eq("ListedEntities::" + processor.getIdentifier()), any());

        assertFalse(processor.isResetTracking());
    }

    @Test
    void testResetEntityTrackingWhenPrefixModified() throws Exception {
        setUpResetTrackingTest(ListGCSBucket.BY_ENTITIES);

        assertFalse(processor.isResetTracking());

        runner.run();

        assertNotNull(processor.getListedEntityTracker());

        runner.setProperty(ListGCSBucket.PREFIX, "prefix2");

        assertTrue(processor.isResetTracking());

        runner.run();

        assertNotNull(processor.getListedEntityTracker());
        verify(mockCache).remove(eq("ListedEntities::" + processor.getIdentifier()), any());

        assertFalse(processor.isResetTracking());
    }

    @Test
    void testResetEntityTrackingWhenStrategyModified() throws Exception {
        setUpResetTrackingTest(ListGCSBucket.BY_ENTITIES);

        assertFalse(processor.isResetTracking());

        runner.run();

        assertNotNull(processor.getListedEntityTracker());

        runner.setProperty(ListGCSBucket.LISTING_STRATEGY, ListGCSBucket.NO_TRACKING);

        assertTrue(processor.isResetTracking());

        runner.run();

        assertNull(processor.getListedEntityTracker());
        verify(mockCache).remove(eq("ListedEntities::" + processor.getIdentifier()), any());

        assertFalse(processor.isResetTracking());
    }

    private void setUpResetTrackingTest(AllowableValue listingStrategy) throws Exception {
        runner.setProperty(ListGCSBucket.LISTING_STRATEGY, listingStrategy);
        runner.setProperty(ListGCSBucket.PREFIX, "prefix1");

        if (listingStrategy == ListGCSBucket.BY_TIMESTAMPS) {
            mockStateManager.setState(Map.of(ListGCSBucket.CURRENT_TIMESTAMP, Long.toString(TIMESTAMP), ListGCSBucket.CURRENT_KEY_PREFIX + "0", "file"), Scope.CLUSTER);
        } else if (listingStrategy == ListGCSBucket.BY_ENTITIES) {
            String serviceId = "DistributedMapCacheClient";
            when(mockCache.getIdentifier()).thenReturn(serviceId);
            runner.addControllerService(serviceId, mockCache);
            runner.enableControllerService(mockCache);
            runner.setProperty(ListGCSBucket.TRACKING_STATE_CACHE, serviceId);
        }

        when(storage.list(anyString(), any(Storage.BlobListOption.class))).thenReturn(new PageImpl<>(null, null, null));
    }

    private Blob buildMockBlob(final String bucket, final String key, final long updateTime) {
        final Blob blob = mock(Blob.class);
        when(blob.getBucket()).thenReturn(bucket);
        when(blob.getName()).thenReturn(key);
        when(blob.getUpdateTimeOffsetDateTime()).thenReturn(offsetDateTime(updateTime));
        when(blob.getCreateTimeOffsetDateTime()).thenReturn(offsetDateTime(updateTime));
        return blob;
    }

    private Blob buildMockBlobWithoutBucket(final String bucket, final String key, final long updateTime) {
        final Blob blob = mock(Blob.class);
        when(blob.getName()).thenReturn(key);
        when(blob.getUpdateTimeOffsetDateTime()).thenReturn(offsetDateTime(updateTime));
        when(blob.getCreateTimeOffsetDateTime()).thenReturn(offsetDateTime(updateTime));
        return blob;
    }

    private OffsetDateTime offsetDateTime(final long value) {
        final Instant instant = Instant.ofEpochMilli(value);
        final LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
        return OffsetDateTime.of(localDateTime, ZoneOffset.UTC);
    }

    private void verifyConfigVerification(final TestRunner runner, final ListGCSBucket processor, final int expectedCount) {
        final List<ConfigVerificationResult> verificationResults = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(3, verificationResults.size());
        final ConfigVerificationResult cloudServiceResult = verificationResults.get(0);
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, cloudServiceResult.getOutcome());

        final ConfigVerificationResult iamPermissionsResult = verificationResults.get(1);
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, iamPermissionsResult.getOutcome());

        final ConfigVerificationResult listingResult = verificationResults.get(2);
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, listingResult.getOutcome());

        assertTrue(
                listingResult.getExplanation().matches(String.format(".*finding %s blobs.*", expectedCount)),
                String.format("Expected %s blobs to be counted, but explanation was: %s", expectedCount, listingResult.getExplanation()));
    }
}
