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
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.util.List;
import java.util.Map;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ListGCSBucket} which do not consume Google Cloud resources.
 */
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

    @Mock
    Storage storage;

    @Captor
    ArgumentCaptor<Storage.BlobListOption> argumentCaptor;

    @Override
    public ListGCSBucket getProcessor() {
        return new ListGCSBucket() {
            @Override
            protected Storage getCloudService() {
                return storage;
            }
        };
    }

    @Override
    protected void addRequiredPropertiesToRunner(TestRunner runner) {
        runner.setProperty(ListGCSBucket.BUCKET, BUCKET);
    }

    @Test
    public void testRestoreFreshState() throws Exception {
        reset(storage);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        assertEquals("Cluster StateMap should be fresh (version -1L)",
                -1L,
                runner.getProcessContext().getStateManager().getState(Scope.CLUSTER).getVersion()
        );

        assertNull(processor.currentKeys);

        processor.restoreState(runner.getProcessContext());

        assertNotNull(processor.currentKeys);
        assertEquals(
                0L,
                processor.currentTimestamp
        );

        assertTrue(processor.currentKeys.isEmpty());

    }

    @Test
    public void testRestorePreviousState() throws Exception {
        reset(storage);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Map<String, String> state = ImmutableMap.of(
                ListGCSBucket.CURRENT_TIMESTAMP, String.valueOf(4L),
                ListGCSBucket.CURRENT_KEY_PREFIX + "0", "test-key-0",
                ListGCSBucket.CURRENT_KEY_PREFIX + "1", "test-key-1"
        );

        runner.getStateManager().setState(state, Scope.CLUSTER);

        assertNull(processor.currentKeys);
        assertEquals(
                0L,
                processor.currentTimestamp
        );

        processor.restoreState(runner.getProcessContext());

        assertNotNull(processor.currentKeys);
        assertTrue(processor.currentKeys.contains("test-key-0"));
        assertTrue(processor.currentKeys.contains("test-key-1"));
        assertEquals(
                4L,
                processor.currentTimestamp
        );
    }

    @Test
    public void testPersistState() throws Exception {
        reset(storage);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        assertEquals("Cluster StateMap should be fresh (version -1L)",
                -1L,
                runner.getProcessContext().getStateManager().getState(Scope.CLUSTER).getVersion()
        );

        processor.currentKeys = ImmutableSet.of(
                "test-key-0",
                "test-key-1"
        );

        processor.currentTimestamp = 4L;

        processor.persistState(runner.getProcessContext());

        final StateMap stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(
                "Cluster StateMap should have been written to",
                1L,
                stateMap.getVersion()
        );

        assertEquals(
                ImmutableMap.of(
                        ListGCSBucket.CURRENT_TIMESTAMP, String.valueOf(4L),
                        ListGCSBucket.CURRENT_KEY_PREFIX+"0", "test-key-0",
                        ListGCSBucket.CURRENT_KEY_PREFIX+"1", "test-key-1"
                ),
                stateMap.toMap()
        );
    }

    @Test
    public void testFailedPersistState() throws Exception {
        reset(storage);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        runner.getStateManager().setFailOnStateSet(Scope.CLUSTER, true);

        processor.currentKeys = ImmutableSet.of(
                "test-key-0",
                "test-key-1"
        );

        processor.currentTimestamp = 4L;

        assertTrue(runner.getLogger().getErrorMessages().isEmpty());

        processor.persistState(runner.getProcessContext());

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

    @Mock
    Page<Blob> mockBlobPages;

    private Blob buildMockBlob(String bucket, String key, long updateTime) {
        final Blob blob = mock(Blob.class);
        when(blob.getBucket()).thenReturn(bucket);
        when(blob.getName()).thenReturn(key);
        when(blob.getUpdateTime()).thenReturn(updateTime);
        return blob;
    }

    @Test
    public void testSuccessfulList() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Iterable<Blob> mockList = ImmutableList.of(
                buildMockBlob("blob-bucket-1", "blob-key-1", 2L),
                buildMockBlob("blob-bucket-2", "blob-key-2", 3L)
        );

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPages);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS);
        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 2);

        final List<MockFlowFile> successes = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);

        MockFlowFile flowFile = successes.get(0);
        assertEquals(
                "blob-bucket-1",
                flowFile.getAttribute(BUCKET_ATTR)
        );

        assertEquals(
                "blob-key-1",
                flowFile.getAttribute(KEY_ATTR)
        );

        assertEquals(
                "2",
                flowFile.getAttribute(UPDATE_TIME_ATTR)
        );

        flowFile = successes.get(1);
        assertEquals(
                "blob-bucket-2",
                flowFile.getAttribute(BUCKET_ATTR)
        );

        assertEquals(
                "blob-key-2",
                flowFile.getAttribute(KEY_ATTR)
        );

        assertEquals(
                "3",
                flowFile.getAttribute(UPDATE_TIME_ATTR)
        );

        assertEquals(
                3L,
                processor.currentTimestamp
        );

        assertEquals(
                ImmutableSet.of(
                        "blob-key-2"
                ),
                processor.currentKeys
        );

    }

    @Test
    public void testOldValues() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Iterable<Blob> mockList = ImmutableList.of(
                buildMockBlob("blob-bucket-1", "blob-key-1", 2L)
        );

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPages);

        runner.enqueue("test");
        runner.enqueue("test2");
        runner.run(2);

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS);
        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 1);

        assertEquals(
                "blob-key-1",
                runner.getStateManager().getState(Scope.CLUSTER).get(ListGCSBucket.CURRENT_KEY_PREFIX+"0")
        );

        assertEquals(
                "2",
                runner.getStateManager().getState(Scope.CLUSTER).get(ListGCSBucket.CURRENT_TIMESTAMP)
        );

    }



    @Test
    public void testEmptyList() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Iterable<Blob> mockList = ImmutableList.of();

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPages);

        runner.enqueue("test");
        runner.run();

        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 0);

        assertEquals(
                "No state should be persisted on an empty return",
                -1L,
                runner.getStateManager().getState(Scope.CLUSTER).getVersion()
        );
    }

    @Test
    public void testAttributesSet() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

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
        when(blob.getCreateTime()).thenReturn(CREATE_TIME);
        when(blob.getUpdateTime()).thenReturn(UPDATE_TIME);

        final Iterable<Blob> mockList = ImmutableList.of(blob);

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPages);

        runner.enqueue("test");
        runner.run();


        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(
                CACHE_CONTROL,
                flowFile.getAttribute(CACHE_CONTROL_ATTR)
        );

        assertEquals(
                COMPONENT_COUNT,
                Integer.valueOf(flowFile.getAttribute(COMPONENT_COUNT_ATTR))
        );

        assertEquals(
                CONTENT_ENCODING,
                flowFile.getAttribute(CONTENT_ENCODING_ATTR)
        );

        assertEquals(
                CONTENT_LANGUAGE,
                flowFile.getAttribute(CONTENT_LANGUAGE_ATTR)
        );

        assertEquals(
                CONTENT_TYPE,
                flowFile.getAttribute(CoreAttributes.MIME_TYPE.key())
        );

        assertEquals(
                CRC32C,
                flowFile.getAttribute(CRC32C_ATTR)
        );

        assertEquals(
                ENCRYPTION,
                flowFile.getAttribute(ENCRYPTION_ALGORITHM_ATTR)
        );

        assertEquals(
                ENCRYPTION_SHA256,
                flowFile.getAttribute(ENCRYPTION_SHA256_ATTR)
        );

        assertEquals(
                ETAG,
                flowFile.getAttribute(ETAG_ATTR)
        );

        assertEquals(
                GENERATED_ID,
                flowFile.getAttribute(GENERATED_ID_ATTR)
        );

        assertEquals(
                GENERATION,
                Long.valueOf(flowFile.getAttribute(GENERATION_ATTR))
        );

        assertEquals(
                MD5,
                flowFile.getAttribute(MD5_ATTR)
        );

        assertEquals(
                MEDIA_LINK,
                flowFile.getAttribute(MEDIA_LINK_ATTR)
        );

        assertEquals(
                METAGENERATION,
                Long.valueOf(flowFile.getAttribute(METAGENERATION_ATTR))
        );

        assertEquals(
                URI,
                flowFile.getAttribute(URI_ATTR)
        );

        assertEquals(
                CONTENT_DISPOSITION,
                flowFile.getAttribute(CONTENT_DISPOSITION_ATTR)
        );

        assertEquals(
                CREATE_TIME,
                Long.valueOf(flowFile.getAttribute(CREATE_TIME_ATTR))
        );

        assertEquals(
                UPDATE_TIME,
                Long.valueOf(flowFile.getAttribute(UPDATE_TIME_ATTR))
        );
    }

    @Test
    public void testAclOwnerUser() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Blob blob = buildMockBlob("test-bucket-1", "test-key-1", 2L);
        final Acl.User mockUser = mock(Acl.User.class);
        when(mockUser.getEmail()).thenReturn(OWNER_USER_EMAIL);
        when(blob.getOwner()).thenReturn(mockUser);

        final Iterable<Blob> mockList = ImmutableList.of(blob);

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPages);

        runner.enqueue("test");
        runner.run();


        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(
                OWNER_USER_EMAIL,
                flowFile.getAttribute(OWNER_ATTR)
        );

        assertEquals(
                "user",
                flowFile.getAttribute(OWNER_TYPE_ATTR)
        );

    }


    @Test
    public void testAclOwnerGroup() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Blob blob = buildMockBlob("test-bucket-1", "test-key-1", 2L);
        final Acl.Group mockGroup = mock(Acl.Group.class);
        when(mockGroup.getEmail()).thenReturn(OWNER_GROUP_EMAIL);
        when(blob.getOwner()).thenReturn(mockGroup);

        final Iterable<Blob> mockList = ImmutableList.of(blob);

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPages);

        runner.enqueue("test");
        runner.run();


        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(
                OWNER_GROUP_EMAIL,
                flowFile.getAttribute(OWNER_ATTR)
        );

        assertEquals(
                "group",
                flowFile.getAttribute(OWNER_TYPE_ATTR)
        );

    }



    @Test
    public void testAclOwnerDomain() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Blob blob = buildMockBlob("test-bucket-1", "test-key-1", 2L);
        final Acl.Domain mockDomain = mock(Acl.Domain.class);
        when(mockDomain.getDomain()).thenReturn(OWNER_DOMAIN);
        when(blob.getOwner()).thenReturn(mockDomain);

        final Iterable<Blob> mockList = ImmutableList.of(blob);

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPages);

        runner.enqueue("test");
        runner.run();


        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(
                OWNER_DOMAIN,
                flowFile.getAttribute(OWNER_ATTR)
        );

        assertEquals(
                "domain",
                flowFile.getAttribute(OWNER_TYPE_ATTR)
        );

    }



    @Test
    public void testAclOwnerProject() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Blob blob = buildMockBlob("test-bucket-1", "test-key-1", 2L);
        final Acl.Project mockProject = mock(Acl.Project.class);
        when(mockProject.getProjectId()).thenReturn(OWNER_PROJECT_ID);
        when(blob.getOwner()).thenReturn(mockProject);

        final Iterable<Blob> mockList = ImmutableList.of(blob);

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPages);

        runner.enqueue("test");
        runner.run();


        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);
        assertEquals(
                OWNER_PROJECT_ID,
                flowFile.getAttribute(OWNER_ATTR)
        );

        assertEquals(
                "project",
                flowFile.getAttribute(OWNER_TYPE_ATTR)
        );

    }


    @Test
    public void testYieldOnBadStateRestore() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Iterable<Blob> mockList = ImmutableList.of();

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), any(Storage.BlobListOption[].class)))
                .thenReturn(mockBlobPages);

        runner.getStateManager().setFailOnStateGet(Scope.CLUSTER, true);
        runner.enqueue("test");
        runner.run();

        runner.assertTransferCount(ListGCSBucket.REL_SUCCESS, 0);
        assertEquals(
                1,
                runner.getLogger().getErrorMessages().size()
        );
    }

    @Test
    public void testListOptionsPrefix() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);

        runner.setProperty(
                ListGCSBucket.PREFIX,
                PREFIX
        );

        runner.assertValid();

        final Iterable<Blob> mockList = ImmutableList.of();

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), argumentCaptor.capture()))
                .thenReturn(mockBlobPages);

        runner.enqueue("test");
        runner.run();

        assertEquals(
                Storage.BlobListOption.prefix(PREFIX),
                argumentCaptor.getValue()
        );

    }


    @Test
    public void testListOptionsVersions() throws Exception {
        reset(storage, mockBlobPages);
        final ListGCSBucket processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);

        runner.setProperty(
                ListGCSBucket.USE_GENERATIONS,
                String.valueOf(USE_GENERATIONS)
        );
        runner.assertValid();

        final Iterable<Blob> mockList = ImmutableList.of();

        when(mockBlobPages.getValues())
                .thenReturn(mockList);

        when(mockBlobPages.getNextPage()).thenReturn(null);

        when(storage.list(anyString(), argumentCaptor.capture()))
                .thenReturn(mockBlobPages);

        runner.enqueue("test");
        runner.run();

        Storage.BlobListOption option = argumentCaptor.getValue();

        assertEquals(
                Storage.BlobListOption.versions(true),
                option
        );
    }
}