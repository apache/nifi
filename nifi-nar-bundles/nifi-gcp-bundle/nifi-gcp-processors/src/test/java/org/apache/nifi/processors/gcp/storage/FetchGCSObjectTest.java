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

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FetchGCSObject}.
 */
public class FetchGCSObjectTest extends AbstractGCSTest {
    private final static String KEY = "test-key";
    private final static Long GENERATION = 5L;
    private static final String CONTENT = "test-content";

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


    @Mock
    Storage storage;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Override
    public AbstractGCSProcessor getProcessor() {
        return new FetchGCSObject() {
            @Override
            protected Storage getCloudService() {
                return storage;
            }
        };
    }

    private class MockReadChannel implements ReadChannel {
        private byte[] toRead;
        private int position = 0;
        private boolean finished;
        private boolean isOpen;

        private MockReadChannel(String textToRead) {
            this.toRead = textToRead.getBytes();
            this.isOpen = true;
            this.finished = false;
        }

        @Override
        public void close() {
            this.isOpen = false;
        }

        @Override
        public void seek(long l) throws IOException {

        }

        @Override
        public void setChunkSize(int i) {

        }

        @Override
        public RestorableState<ReadChannel> capture() {
            return null;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (this.finished) {
                return -1;
            } else {
                if (dst.remaining() > this.toRead.length) {
                    this.finished = true;
                }
                int toWrite = Math.min(this.toRead.length - position, dst.remaining());

                dst.put(this.toRead, this.position, toWrite);
                this.position += toWrite;

                return toWrite;
            }
        }

        @Override
        public boolean isOpen() {
            return this.isOpen;
        }
    }

    @Override
    protected void addRequiredPropertiesToRunner(TestRunner runner) {
        runner.setProperty(FetchGCSObject.BUCKET, BUCKET);
        runner.setProperty(FetchGCSObject.KEY, String.valueOf(KEY));
    }

    @Test
    public void testSuccessfulFetch() throws Exception {
        reset(storage);
        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Blob blob = mock(Blob.class);

        when(blob.getBucket()).thenReturn(BUCKET);
        when(blob.getName()).thenReturn(KEY);
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

        when(storage.get(any(BlobId.class))).thenReturn(blob);
        when(storage.reader(any(BlobId.class), any(Storage.BlobSourceOption.class))).thenReturn(new MockReadChannel(CONTENT));


        runner.enqueue("");

        runner.run();

        verify(storage).get(any(BlobId.class));
        verify(storage).reader(any(BlobId.class), any(Storage.BlobSourceOption.class));

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS);
        runner.assertTransferCount(FetchGCSObject.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS).get(0);

        assertTrue(flowFile.isContentEqual(CONTENT));
        assertEquals(
                BUCKET,
                flowFile.getAttribute(BUCKET_ATTR)
        );

        assertEquals(
                KEY,
                flowFile.getAttribute(KEY_ATTR)
        );

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
        reset(storage);
        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Blob blob = mock(Blob.class);

        final Acl.User mockUser = mock(Acl.User.class);
        when(mockUser.getEmail()).thenReturn(OWNER_USER_EMAIL);
        when(blob.getOwner()).thenReturn(mockUser);

        when(storage.get(any(BlobId.class))).thenReturn(blob);
        when(storage.reader(any(BlobId.class), any(Storage.BlobSourceOption.class))).thenReturn(new MockReadChannel(CONTENT));

        runner.enqueue("");

        runner.run();

        verify(storage).get(any(BlobId.class));
        verify(storage).reader(any(BlobId.class), any(Storage.BlobSourceOption.class));

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
        reset(storage);
        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Blob blob = mock(Blob.class);

        final Acl.Group mockGroup = mock(Acl.Group.class);
        when(mockGroup.getEmail()).thenReturn(OWNER_GROUP_EMAIL);
        when(blob.getOwner()).thenReturn(mockGroup);

        when(storage.get(any(BlobId.class))).thenReturn(blob);
        when(storage.reader(any(BlobId.class), any(Storage.BlobSourceOption.class))).thenReturn(new MockReadChannel(CONTENT));


        runner.enqueue("");

        runner.run();

        verify(storage).get(any(BlobId.class));
        verify(storage).reader(any(BlobId.class), any(Storage.BlobSourceOption.class));

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
        reset(storage);
        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Blob blob = mock(Blob.class);

        final Acl.Domain mockDomain = mock(Acl.Domain.class);
        when(mockDomain.getDomain()).thenReturn(OWNER_DOMAIN);
        when(blob.getOwner()).thenReturn(mockDomain);

        when(storage.get(any(BlobId.class))).thenReturn(blob);
        when(storage.reader(any(BlobId.class), any(Storage.BlobSourceOption.class))).thenReturn(new MockReadChannel(CONTENT));


        runner.enqueue("");

        runner.run();

        verify(storage).get(any(BlobId.class));
        verify(storage).reader(any(BlobId.class), any(Storage.BlobSourceOption.class));

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
        reset(storage);
        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Blob blob = mock(Blob.class);
        final Acl.Project mockProject = mock(Acl.Project.class);
        when(mockProject.getProjectId()).thenReturn(OWNER_PROJECT_ID);
        when(blob.getOwner()).thenReturn(mockProject);

        when(storage.get(any(BlobId.class))).thenReturn(blob);
        when(storage.reader(any(BlobId.class), any(Storage.BlobSourceOption.class))).thenReturn(new MockReadChannel(CONTENT));

        runner.enqueue("");

        runner.run();

        verify(storage).get(any(BlobId.class));
        verify(storage).reader(any(BlobId.class), any(Storage.BlobSourceOption.class));

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
    public void testBlobIdWithGeneration() throws Exception {
        reset(storage);
        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);

        runner.removeProperty(FetchGCSObject.KEY);
        runner.removeProperty(FetchGCSObject.BUCKET);

        runner.setProperty(FetchGCSObject.GENERATION, String.valueOf(GENERATION));
        runner.assertValid();

        final Blob blob = mock(Blob.class);
        when(storage.get(any(BlobId.class))).thenReturn(blob);
        when(storage.reader(any(BlobId.class), any(Storage.BlobSourceOption.class))).thenReturn(new MockReadChannel(CONTENT));

        runner.enqueue("", ImmutableMap.of(
                BUCKET_ATTR, BUCKET,
                CoreAttributes.FILENAME.key(), KEY
        ));

        runner.run();

        ArgumentCaptor<BlobId> blobIdArgumentCaptor = ArgumentCaptor.forClass(BlobId.class);
        ArgumentCaptor<Storage.BlobSourceOption> blobSourceOptionArgumentCaptor = ArgumentCaptor.forClass(Storage.BlobSourceOption.class);
        verify(storage).get(blobIdArgumentCaptor.capture());
        verify(storage).reader(any(BlobId.class), blobSourceOptionArgumentCaptor.capture());

        final BlobId blobId = blobIdArgumentCaptor.getValue();

        assertEquals(
                BUCKET,
                blobId.getBucket()
        );

        assertEquals(
                KEY,
                blobId.getName()
        );

        assertEquals(
                GENERATION,
                blobId.getGeneration()
        );


        final Set<Storage.BlobSourceOption> blobSourceOptions = ImmutableSet.copyOf(blobSourceOptionArgumentCaptor.getAllValues());
        assertTrue(blobSourceOptions.contains(Storage.BlobSourceOption.generationMatch()));
        assertEquals(
                1,
                blobSourceOptions.size()
        );

    }


    @Test
    public void testBlobIdWithEncryption() throws Exception {
        reset(storage);
        final TestRunner runner = buildNewRunner(getProcessor());

        runner.setProperty(FetchGCSObject.ENCRYPTION_KEY, ENCRYPTION_SHA256);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final Blob blob = mock(Blob.class);
        when(storage.get(any(BlobId.class))).thenReturn(blob);
        when(storage.reader(any(BlobId.class), any(Storage.BlobSourceOption.class))).thenReturn(new MockReadChannel(CONTENT));

        runner.enqueue("");

        runner.run();

        ArgumentCaptor<BlobId> blobIdArgumentCaptor = ArgumentCaptor.forClass(BlobId.class);
        ArgumentCaptor<Storage.BlobSourceOption> blobSourceOptionArgumentCaptor = ArgumentCaptor.forClass(Storage.BlobSourceOption.class);
        verify(storage).get(blobIdArgumentCaptor.capture());
        verify(storage).reader(any(BlobId.class), blobSourceOptionArgumentCaptor.capture());

        final BlobId blobId = blobIdArgumentCaptor.getValue();

        assertEquals(
                BUCKET,
                blobId.getBucket()
        );

        assertEquals(
                KEY,
                blobId.getName()
        );

        assertNull(blobId.getGeneration());

        final Set<Storage.BlobSourceOption> blobSourceOptions = ImmutableSet.copyOf(blobSourceOptionArgumentCaptor.getAllValues());

        assertTrue(blobSourceOptions.contains(Storage.BlobSourceOption.decryptionKey(ENCRYPTION_SHA256)));
        assertEquals(
                1,
                blobSourceOptions.size()
        );
    }

    @Test
    public void testStorageExceptionOnFetch() throws Exception {
        reset(storage);
        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        when(storage.get(any(BlobId.class))).thenThrow(new StorageException(400, "test-exception"));
        when(storage.reader(any(BlobId.class), any(Storage.BlobSourceOption.class))).thenReturn(new MockReadChannel(CONTENT));

        runner.enqueue("");

        runner.run();

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_FAILURE);
        runner.assertTransferCount(FetchGCSObject.REL_FAILURE, 1);
    }
}
