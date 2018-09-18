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

import static com.google.cloud.storage.Storage.PredefinedAcl.BUCKET_OWNER_READ;
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
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.SIZE_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.UPDATE_TIME_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.URI_ATTR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

/**
 * Unit tests for {@link PutGCSObject} which do not use Google Cloud resources.
 */
@SuppressWarnings("deprecation")
public class PutGCSObjectTest extends AbstractGCSTest {
    private static final String FILENAME = "test-filename";
    private static final String KEY = "test-key";
    private static final String CONTENT_TYPE = "test-content-type";
    private static final String MD5 = "test-md5";
    private static final String CRC32C = "test-crc32c";
    private static final Storage.PredefinedAcl ACL = BUCKET_OWNER_READ;
    private static final String ENCRYPTION_KEY = "test-encryption-key";
    private static final Boolean OVERWRITE = false;
    private static final String CONTENT_DISPOSITION_TYPE = "inline";


    private static final Long SIZE = 100L;
    private static final String CACHE_CONTROL = "test-cache-control";
    private static final Integer COMPONENT_COUNT = 3;
    private static final String CONTENT_ENCODING = "test-content-encoding";
    private static final String CONTENT_LANGUAGE = "test-content-language";
    private static final String ENCRYPTION = "test-encryption";
    private static final String ENCRYPTION_SHA256 = "test-encryption-256";
    private static final String ETAG = "test-etag";
    private static final String GENERATED_ID = "test-generated-id";
    private static final String MEDIA_LINK = "test-media-link";
    private static final Long METAGENERATION = 42L;
    private static final String OWNER_USER_EMAIL = "test-owner-user-email";
    private static final String OWNER_GROUP_EMAIL = "test-owner-group-email";
    private static final String OWNER_DOMAIN = "test-owner-domain";
    private static final String OWNER_PROJECT_ID = "test-owner-project-id";
    private static final String URI = "test-uri";
    private static final String CONTENT_DISPOSITION = "attachment; filename=\"" + FILENAME + "\"";
    private static final Long CREATE_TIME = 1234L;
    private static final Long UPDATE_TIME = 4567L;
    private final static Long GENERATION = 5L;

    @Mock
    Storage storage;

    @Mock
    Blob blob;

    @Captor
    ArgumentCaptor<Storage.BlobWriteOption> blobWriteOptionArgumentCaptor;

    @Captor
    ArgumentCaptor<InputStream> inputStreamArgumentCaptor;

    @Captor
    ArgumentCaptor<BlobInfo> blobInfoArgumentCaptor;

    @Override
    public PutGCSObject getProcessor() {
        return new PutGCSObject() {
            @Override
            protected Storage getCloudService() {
                return storage;
            }
        };
    }

    @Override
    protected void addRequiredPropertiesToRunner(TestRunner runner) {
        runner.setProperty(PutGCSObject.BUCKET, BUCKET);
    }

    @Test
    public void testSuccessfulPutOperationNoParameters() throws Exception {
        reset(storage, blob);
        final PutGCSObject processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);

        runner.assertValid();

        when(storage.create(blobInfoArgumentCaptor.capture(),
                inputStreamArgumentCaptor.capture(),
                blobWriteOptionArgumentCaptor.capture())).thenReturn(blob);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutGCSObject.REL_SUCCESS);
        runner.assertTransferCount(PutGCSObject.REL_SUCCESS, 1);

        /** Can't do this any more due to the switch to Java InputStreams which close after an operation **/
        /*
        String text;
        try (final Reader reader = new InputStreamReader(inputStreamArgumentCaptor.getValue())) {
            text = CharStreams.toString(reader);
        }

        assertEquals(
                "FlowFile content should be equal to the Blob content",
                "test",
                text
        );
        */

        final List<Storage.BlobWriteOption> blobWriteOptions = blobWriteOptionArgumentCaptor.getAllValues();
        assertEquals("No BlobWriteOptions should be set",
                0,
                blobWriteOptions.size());

        final BlobInfo blobInfo = blobInfoArgumentCaptor.getValue();
        assertNull(blobInfo.getMd5());
        assertNull(blobInfo.getContentDisposition());
        assertNull(blobInfo.getCrc32c());
    }

    @Test
    public void testSuccessfulPutOperation() throws Exception {
        reset(storage, blob);
        final PutGCSObject processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);

        runner.setProperty(PutGCSObject.KEY, KEY);
        runner.setProperty(PutGCSObject.CONTENT_TYPE, CONTENT_TYPE);
        runner.setProperty(PutGCSObject.MD5, MD5);
        runner.setProperty(PutGCSObject.CRC32C, CRC32C);
        runner.setProperty(PutGCSObject.ACL, ACL.name());
        runner.setProperty(PutGCSObject.ENCRYPTION_KEY, ENCRYPTION_KEY);
        runner.setProperty(PutGCSObject.OVERWRITE, String.valueOf(OVERWRITE));
        runner.setProperty(PutGCSObject.CONTENT_DISPOSITION_TYPE, CONTENT_DISPOSITION_TYPE);

        runner.assertValid();

        when(storage.create(blobInfoArgumentCaptor.capture(),
                inputStreamArgumentCaptor.capture(),
                blobWriteOptionArgumentCaptor.capture())).thenReturn(blob);

        runner.enqueue("test", ImmutableMap.of(CoreAttributes.FILENAME.key(), FILENAME));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutGCSObject.REL_SUCCESS);
        runner.assertTransferCount(PutGCSObject.REL_SUCCESS, 1);

        /*

        String text;
        try (final Reader reader = new InputStreamReader(inputStreamArgumentCaptor.getValue())) {
            text = CharStreams.toString(reader);
        }

        assertEquals(
                "FlowFile content should be equal to the Blob content",
                "test",
                text
        );

        */

        final BlobInfo blobInfo = blobInfoArgumentCaptor.getValue();
        assertEquals(
                BUCKET,
                blobInfo.getBucket()
        );

        assertEquals(
                KEY,
                blobInfo.getName()
        );

        assertEquals(
                CONTENT_DISPOSITION_TYPE + "; filename=" + FILENAME,
                blobInfo.getContentDisposition()
        );

        assertEquals(
                CONTENT_TYPE,
                blobInfo.getContentType()
        );

        assertEquals(
                MD5,
                blobInfo.getMd5()
        );

        assertEquals(
                CRC32C,
                blobInfo.getCrc32c()
        );

        assertNull(blobInfo.getMetadata());

        final List<Storage.BlobWriteOption> blobWriteOptions = blobWriteOptionArgumentCaptor.getAllValues();
        final Set<Storage.BlobWriteOption> blobWriteOptionSet = ImmutableSet.copyOf(blobWriteOptions);

        assertEquals(
                "Each of the BlobWriteOptions should be unique",
                blobWriteOptions.size(),
                blobWriteOptionSet.size()
        );

        assertTrue("The doesNotExist BlobWriteOption should be set if OVERWRITE is false",
                blobWriteOptionSet.contains(Storage.BlobWriteOption.doesNotExist()));
        assertTrue("The md5Match BlobWriteOption should be set if MD5 is non-null",
                blobWriteOptionSet.contains(Storage.BlobWriteOption.md5Match()));
        assertTrue("The crc32cMatch BlobWriteOption should be set if CRC32C is non-null",
                blobWriteOptionSet.contains(Storage.BlobWriteOption.crc32cMatch()));
        assertTrue("The predefinedAcl BlobWriteOption should be set if ACL is non-null",
                blobWriteOptionSet.contains(Storage.BlobWriteOption.predefinedAcl(ACL)));
        assertTrue("The encryptionKey BlobWriteOption should be set if ENCRYPTION_KEY is non-null",
                blobWriteOptionSet.contains(Storage.BlobWriteOption.encryptionKey(ENCRYPTION_KEY)));

    }

    @Test
    public void testSuccessfulPutOperationWithUserMetadata() throws Exception {
        reset(storage, blob);
        final PutGCSObject processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);

        runner.setProperty(
                "testMetadataKey1", "testMetadataValue1"
        );
        runner.setProperty(
                "testMetadataKey2", "testMetadataValue2"
        );

        runner.assertValid();

        when(storage.create(blobInfoArgumentCaptor.capture(),
                inputStreamArgumentCaptor.capture(),
                blobWriteOptionArgumentCaptor.capture())).thenReturn(blob);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutGCSObject.REL_SUCCESS);
        runner.assertTransferCount(PutGCSObject.REL_SUCCESS, 1);


        /*
        String text;
        try (final Reader reader = new InputStreamReader(inputStreamArgumentCaptor.getValue())) {
            text = CharStreams.toString(reader);
        }

        assertEquals(
                "FlowFile content should be equal to the Blob content",
                "test",
                text
        );

        */

        final BlobInfo blobInfo = blobInfoArgumentCaptor.getValue();
        final Map<String, String> metadata = blobInfo.getMetadata();

        assertNotNull(metadata);

        assertEquals(
                2,
                metadata.size()
        );

        assertEquals(
                "testMetadataValue1",
                metadata.get("testMetadataKey1")
        );

        assertEquals(
                "testMetadataValue2",
                metadata.get("testMetadataKey2")
        );
    }

    @Test
    public void testAttributesSetOnSuccessfulPut() throws Exception {
        reset(storage, blob);
        final PutGCSObject processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        when(storage.create(any(BlobInfo.class), any(InputStream.class), any(Storage.BlobWriteOption.class)))
                .thenReturn(blob);

        when(blob.getBucket()).thenReturn(BUCKET);
        when(blob.getName()).thenReturn(KEY);
        when(blob.getSize()).thenReturn(SIZE);
        when(blob.getCacheControl()).thenReturn(CACHE_CONTROL);
        when(blob.getComponentCount()).thenReturn(COMPONENT_COUNT);
        when(blob.getContentDisposition()).thenReturn(CONTENT_DISPOSITION);
        when(blob.getContentEncoding()).thenReturn(CONTENT_ENCODING);
        when(blob.getContentLanguage()).thenReturn(CONTENT_LANGUAGE);
        when(blob.getContentType()).thenReturn(CONTENT_TYPE);
        when(blob.getCrc32c()).thenReturn(CRC32C);

        final BlobInfo.CustomerEncryption mockEncryption = mock(BlobInfo.CustomerEncryption.class);
        when(blob.getCustomerEncryption()).thenReturn(mockEncryption);
        when(mockEncryption.getEncryptionAlgorithm()).thenReturn(ENCRYPTION);
        when(mockEncryption.getKeySha256()).thenReturn(ENCRYPTION_SHA256);
        when(blob.getEtag()).thenReturn(ETAG);
        when(blob.getGeneratedId()).thenReturn(GENERATED_ID);
        when(blob.getGeneration()).thenReturn(GENERATION);
        when(blob.getMd5()).thenReturn(MD5);
        when(blob.getMediaLink()).thenReturn(MEDIA_LINK);
        when(blob.getMetageneration()).thenReturn(METAGENERATION);
        when(blob.getSelfLink()).thenReturn(URI);
        when(blob.getCreateTime()).thenReturn(CREATE_TIME);
        when(blob.getUpdateTime()).thenReturn(UPDATE_TIME);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutGCSObject.REL_SUCCESS);
        runner.assertTransferCount(PutGCSObject.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutGCSObject.REL_SUCCESS).get(0);

        mockFlowFile.assertAttributeEquals(BUCKET_ATTR, BUCKET);
        mockFlowFile.assertAttributeEquals(KEY_ATTR, KEY);
        mockFlowFile.assertAttributeEquals(SIZE_ATTR, String.valueOf(SIZE));
        mockFlowFile.assertAttributeEquals(CACHE_CONTROL_ATTR, CACHE_CONTROL);
        mockFlowFile.assertAttributeEquals(COMPONENT_COUNT_ATTR, String.valueOf(COMPONENT_COUNT));
        mockFlowFile.assertAttributeEquals(CONTENT_DISPOSITION_ATTR, CONTENT_DISPOSITION);
        mockFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), FILENAME);
        mockFlowFile.assertAttributeEquals(CONTENT_ENCODING_ATTR, CONTENT_ENCODING);
        mockFlowFile.assertAttributeEquals(CONTENT_LANGUAGE_ATTR, CONTENT_LANGUAGE);
        mockFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CONTENT_TYPE);
        mockFlowFile.assertAttributeEquals(CRC32C_ATTR, CRC32C);
        mockFlowFile.assertAttributeEquals(ENCRYPTION_ALGORITHM_ATTR, ENCRYPTION);
        mockFlowFile.assertAttributeEquals(ENCRYPTION_SHA256_ATTR, ENCRYPTION_SHA256);
        mockFlowFile.assertAttributeEquals(ETAG_ATTR, ETAG);
        mockFlowFile.assertAttributeEquals(GENERATED_ID_ATTR, GENERATED_ID);
        mockFlowFile.assertAttributeEquals(GENERATION_ATTR, String.valueOf(GENERATION));
        mockFlowFile.assertAttributeEquals(MD5_ATTR, MD5);
        mockFlowFile.assertAttributeEquals(MEDIA_LINK_ATTR, MEDIA_LINK);
        mockFlowFile.assertAttributeEquals(METAGENERATION_ATTR, String.valueOf(METAGENERATION));
        mockFlowFile.assertAttributeEquals(URI_ATTR, URI);
        mockFlowFile.assertAttributeEquals(CREATE_TIME_ATTR, String.valueOf(CREATE_TIME));
        mockFlowFile.assertAttributeEquals(UPDATE_TIME_ATTR, String.valueOf(UPDATE_TIME));
    }

    @Test
    public void testAclAttributeUser() throws Exception {
        reset(storage, blob);
        final PutGCSObject processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        when(storage.create(any(BlobInfo.class), any(InputStream.class), any(Storage.BlobWriteOption.class)))
                .thenReturn(blob);

        final Acl.User mockUser = mock(Acl.User.class);
        when(mockUser.getEmail()).thenReturn(OWNER_USER_EMAIL);
        when(blob.getOwner()).thenReturn(mockUser);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutGCSObject.REL_SUCCESS);
        runner.assertTransferCount(PutGCSObject.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutGCSObject.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(OWNER_ATTR, OWNER_USER_EMAIL);
        mockFlowFile.assertAttributeEquals(OWNER_TYPE_ATTR, "user");
    }

    @Test
    public void testAclAttributeGroup() throws Exception {
        reset(storage, blob);
        final PutGCSObject processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        when(storage.create(any(BlobInfo.class), any(InputStream.class), any(Storage.BlobWriteOption.class)))
                .thenReturn(blob);

        final Acl.Group mockGroup = mock(Acl.Group.class);
        when(mockGroup.getEmail()).thenReturn(OWNER_GROUP_EMAIL);
        when(blob.getOwner()).thenReturn(mockGroup);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutGCSObject.REL_SUCCESS);
        runner.assertTransferCount(PutGCSObject.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutGCSObject.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(OWNER_ATTR, OWNER_GROUP_EMAIL);
        mockFlowFile.assertAttributeEquals(OWNER_TYPE_ATTR, "group");
    }


    @Test
    public void testAclAttributeDomain() throws Exception {
        reset(storage, blob);
        final PutGCSObject processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        when(storage.create(any(BlobInfo.class), any(InputStream.class), any(Storage.BlobWriteOption.class)))
                .thenReturn(blob);

        final Acl.Domain mockDomain = mock(Acl.Domain.class);
        when(mockDomain.getDomain()).thenReturn(OWNER_DOMAIN);
        when(blob.getOwner()).thenReturn(mockDomain);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutGCSObject.REL_SUCCESS);
        runner.assertTransferCount(PutGCSObject.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutGCSObject.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(OWNER_ATTR, OWNER_DOMAIN);
        mockFlowFile.assertAttributeEquals(OWNER_TYPE_ATTR, "domain");
    }


    @Test
    public void testAclAttributeProject() throws Exception {
        reset(storage, blob);
        final PutGCSObject processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        when(storage.create(any(BlobInfo.class), any(InputStream.class), any(Storage.BlobWriteOption.class)))
                .thenReturn(blob);

        final Acl.Project mockProject = mock(Acl.Project.class);
        when(mockProject.getProjectId()).thenReturn(OWNER_PROJECT_ID);
        when(blob.getOwner()).thenReturn(mockProject);

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutGCSObject.REL_SUCCESS);
        runner.assertTransferCount(PutGCSObject.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutGCSObject.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(OWNER_ATTR, OWNER_PROJECT_ID);
        mockFlowFile.assertAttributeEquals(OWNER_TYPE_ATTR, "project");
    }

    @Test
    public void testFailureHandling() throws Exception {
        reset(storage);
        final PutGCSObject processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        when(storage.create(any(BlobInfo.class), any(InputStream.class), any(Storage.BlobWriteOption.class)))
                .thenThrow(new StorageException(404, "test exception"));

        runner.enqueue("test");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutGCSObject.REL_FAILURE);
        runner.assertTransferCount(PutGCSObject.REL_FAILURE, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutGCSObject.REL_FAILURE).get(0);
        assertTrue(mockFlowFile.isPenalized());
    }

}