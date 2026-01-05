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

import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.fileresource.service.StandardFileResourceService;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.processors.aws.s3.encryption.StandardS3EncryptionService;
import org.apache.nifi.processors.transfer.ResourceTransferSource;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tag;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.nifi.processors.transfer.ResourceTransferProperties.FILE_RESOURCE_SERVICE;
import static org.apache.nifi.processors.transfer.ResourceTransferProperties.RESOURCE_TRANSFER_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Makes use of Localstack TestContainer in order to test S3 integrations
 */
public class ITPutS3Object extends AbstractS3IT {

    static final String TEST_PARTSIZE_STRING = "50 mb";
    static final Long   TEST_PARTSIZE_LONG = 50L * 1024L * 1024L;

    static final Long S3_MINIMUM_PART_SIZE = 50L * 1024L * 1024L;
    static final Long S3_MAXIMUM_OBJECT_SIZE = 5L * 1024L * 1024L * 1024L;

    static final Pattern reS3ETag = Pattern.compile("[0-9a-fA-f]{32}(-[0-9]+)?");

    private static final Logger logger = LoggerFactory.getLogger(ITPutS3Object.class);
    private static String kmsKeyId = "";
    private static String randomKeyMaterial = "";


    @BeforeAll
    public static void setupClass() {
        byte[] keyRawBytes = new byte[32];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(keyRawBytes);

        randomKeyMaterial = Base64.encodeBase64String(keyRawBytes);
        kmsKeyId = getKMSKey();
    }

    @Test
    public void testPutFromLocalFile() throws Exception {
        TestRunner runner = initTestRunner();
        String attributeName = "file.path";
        Path resourcePath = getResourcePath(SAMPLE_FILE_RESOURCE_NAME);

        String serviceId = FileResourceService.class.getSimpleName();
        FileResourceService service = new StandardFileResourceService();
        runner.addControllerService(serviceId, service);
        runner.setProperty(service, StandardFileResourceService.FILE_PATH, String.format("${%s}", attributeName));
        runner.enableControllerService(service);

        runner.setProperty(RESOURCE_TRANSFER_SOURCE, ResourceTransferSource.FILE_RESOURCE_SERVICE.getValue());
        runner.setProperty(FILE_RESOURCE_SERVICE, serviceId);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(attributeName, resourcePath.toString());
        runner.enqueue(resourcePath, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        ListObjectsRequest listRequest = ListObjectsRequest.builder().bucket(BUCKET_NAME).build();
        List<S3Object> objects = getClient().listObjects(listRequest).contents();
        assertEquals(1, objects.size());
        assertEquals(objects.getFirst().key(), resourcePath.getFileName().toString());
        assertNotEquals(0, objects.getFirst().size());
    }

    @Test
    public void testPutFromNonExistentLocalFile() throws Exception {
        TestRunner runner = initTestRunner();
        String attributeName = "file.path";

        String serviceId = FileResourceService.class.getSimpleName();
        FileResourceService service = new StandardFileResourceService();
        runner.addControllerService(serviceId, service);
        runner.setProperty(service, StandardFileResourceService.FILE_PATH, String.format("${%s}", attributeName));
        runner.enableControllerService(service);

        runner.setProperty(RESOURCE_TRANSFER_SOURCE, ResourceTransferSource.FILE_RESOURCE_SERVICE);
        runner.setProperty(FILE_RESOURCE_SERVICE, serviceId);

        String filePath = "nonexistent.txt";

        Map<String, String> attributes = new HashMap<>();
        attributes.put(attributeName, filePath);

        runner.enqueue("", attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_FAILURE, 1);
        ListObjectsRequest listRequest = ListObjectsRequest.builder().bucket(BUCKET_NAME).build();
        assertTrue(getClient().listObjects(listRequest).contents().isEmpty());
    }

    @Test
    public void testSimplePut() throws IOException {
        TestRunner runner = initTestRunner();

        assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        for (int i = 0; i < 3; i++) {
            final Map<String, String> attrs = new HashMap<>();
            attrs.put("filename", i + ".txt");
            runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        }
        runner.run(3);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 3);
    }

    @Test
    public void testSimplePutFilenameWithNationalCharacters() throws IOException {
        TestRunner runner = initTestRunner();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "Iñtërnâtiônàližætiøn.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testPutS3ObjectUsingCredentialsProviderService() throws Throwable {
        final TestRunner runner = initTestRunner();
        assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        for (int i = 0; i < 3; i++) {
            final Map<String, String> attrs = new HashMap<>();
            attrs.put("filename", i + ".txt");
            runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        }
        runner.run(3);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 3);
    }

    @Test
    public void testMetaData() throws IOException {
        final TestRunner runner = initTestRunner();

        runner.setProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        runner.setProperty("TEST-PROP-1", "TESTING-1-2-3");
        runner.setProperty("TEST-PROP-2", "TESTING-4-5-6");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "meta.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        MockFlowFile ff = flowFiles.get(0);
        ff.assertAttributeExists("s3.usermetadata");
    }

    @Test
    public void testContentType() throws IOException {
        TestRunner runner = initTestRunner();

        runner.setProperty(PutS3Object.CONTENT_TYPE, "text/plain");

        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertAttributeEquals(PutS3Object.S3_CONTENT_TYPE, "text/plain");
    }

    @Test
    public void testContentDispositionInline() throws IOException {
        TestRunner runner = initTestRunner();

        runner.setProperty(PutS3Object.CONTENT_DISPOSITION, PutS3Object.CONTENT_DISPOSITION_INLINE);

        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertAttributeEquals(PutS3Object.S3_CONTENT_DISPOSITION, PutS3Object.CONTENT_DISPOSITION_INLINE);
    }

    @Test
    public void testContentDispositionNull() throws IOException {
        // Put
        TestRunner runner = initTestRunner();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename",  "filename-on-s3.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);

        // Fetch
        runner = initFetchRunner();
        runner.enqueue(new byte[0], attrs);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);
        ff.assertContentEquals(getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        ff.assertAttributeNotExists(PutS3Object.S3_CONTENT_DISPOSITION);
    }

    @Test
    public void testContentDispositionAttachment() throws IOException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutS3Object.CONTENT_DISPOSITION, PutS3Object.CONTENT_DISPOSITION_ATTACHMENT);
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        final MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertAttributeEquals(PutS3Object.S3_CONTENT_DISPOSITION, "attachment; filename=\"hello.txt\"");
    }

    @Test
    public void testCacheControl() throws IOException {
        TestRunner runner = initTestRunner();

        runner.setProperty(PutS3Object.CACHE_CONTROL, "no-cache");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertAttributeEquals(PutS3Object.S3_CACHE_CONTROL, "no-cache");
    }

    @Test
    public void testPutInFolder() throws IOException {
        TestRunner runner = initTestRunner();

        assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());
        runner.assertValid();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/1.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }


    @Test
    public void testPermissions() throws IOException {
        TestRunner runner = initTestRunner();

        runner.setProperty(PutS3Object.FULL_CONTROL_USER_LIST, "28545acd76c35c7e91f8409b95fd1aa0c0914bfa1ac60975d9f48bc3c5e090b5");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/4.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testDynamicProperty() {
        final String DYNAMIC_ATTRIB_KEY = "fs.runTimestamp";
        final String DYNAMIC_ATTRIB_VALUE = "${now():toNumber()}";

        final TestRunner runner = initTestRunner();

        runner.setProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);
        runner.setProperty(DYNAMIC_ATTRIB_KEY, DYNAMIC_ATTRIB_VALUE);

        final String FILE1_NAME = "file1";
        Map<String, String> attribs = new HashMap<>();
        attribs.put(CoreAttributes.FILENAME.key(), FILE1_NAME);
        runner.enqueue("123".getBytes(), attribs);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, successFiles.size());
        MockFlowFile ff1 = successFiles.get(0);

        long now = System.currentTimeMillis();
        String millisNow = Long.toString(now);
        String millisOneSecAgo = Long.toString(now - 1000L);
        String usermeta = ff1.getAttribute(PutS3Object.S3_USERMETA_ATTR_KEY);
        String[] usermetaLine0 = usermeta.split(System.lineSeparator())[0].split("=");
        String usermetaKey0 = usermetaLine0[0];
        String usermetaValue0 = usermetaLine0[1];
        assertEquals(DYNAMIC_ATTRIB_KEY, usermetaKey0);
        assertTrue(usermetaValue0.compareTo(millisOneSecAgo) >= 0 && usermetaValue0.compareTo(millisNow) <= 0);
    }

    @Test
    public void testProvenance() throws InitializationException {
        final String PROV1_FILE = "provfile1";

        final TestRunner runner = initTestRunner();

        setSecureProperties(runner);
        runner.setProperty(RegionUtil.REGION, getRegion());
        runner.setProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        runner.setProperty(PutS3Object.KEY, "${filename}");

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), PROV1_FILE);
        runner.enqueue("prov1 contents".getBytes(), attributes);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, successFiles.size());

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        ProvenanceEventRecord provRec1 = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.SEND, provRec1.getEventType());
        assertEquals(runner.getProcessor().getIdentifier(), provRec1.getComponentId());
        String targetUri = String.format("%s/%s/%s", getEndpoint(), BUCKET_NAME, PROV1_FILE);
        assertEquals(targetUri, provRec1.getTransitUri());
        assertEquals(BUCKET_NAME, provRec1.getUpdatedAttributes().get(PutS3Object.S3_BUCKET_KEY));
    }

    @Test
    public void testStateDefaults() {
        PutS3Object.MultipartState state1 = new PutS3Object.MultipartState();
        assertEquals("", state1.getUploadId());
        assertEquals((Long) 0L, state1.getFilePosition());
        assertEquals(0L, state1.getCompletedParts().size());
        assertEquals((Long) 0L, state1.getPartSize());
        assertEquals(StorageClass.STANDARD, state1.getStorageClass());
        assertEquals((Long) 0L, state1.getContentLength());
    }

    @Test
    public void testStateToString() {
        final String target = "UID-test1234567890#10001#1/PartETag-1,2/PartETag-2,3/PartETag-3,4/PartETag-4#20002#REDUCED_REDUNDANCY#30003#8675309";
        PutS3Object.MultipartState state2 = new PutS3Object.MultipartState();
        state2.setUploadId("UID-test1234567890");
        state2.setFilePosition(10001L);
        state2.setTimestamp(8675309L);
        for (int partNum = 1; partNum < 5; partNum++) {
            state2.addCompletedPart(CompletedPart.builder().partNumber(partNum).eTag("PartETag-" + partNum).build());
        }
        state2.setPartSize(20002L);
        state2.setStorageClass(StorageClass.REDUCED_REDUNDANCY);
        state2.setContentLength(30003L);
        assertEquals(target, state2.toString());
    }

    @Test
    public void testMultipartProperties() {
        final TestRunner runner = initTestRunner();
        final ProcessContext context = runner.getProcessContext();

        runner.setProperty(PutS3Object.FULL_CONTROL_USER_LIST, "28545acd76c35c7e91f8409b95fd1aa0c0914bfa1ac60975d9f48bc3c5e090b5");
        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);
        runner.setProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        runner.setProperty(PutS3Object.KEY, AbstractS3IT.SAMPLE_FILE_RESOURCE_NAME);

        assertEquals(BUCKET_NAME, context.getProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions((FlowFile) null).toString());
        assertEquals(SAMPLE_FILE_RESOURCE_NAME, context.getProperty(PutS3Object.KEY).evaluateAttributeExpressions(Collections.emptyMap()).toString());
        assertEquals(TEST_PARTSIZE_LONG.longValue(),
            context.getProperty(PutS3Object.MULTIPART_PART_SIZE).asDataSize(DataUnit.B).longValue());
    }

    @Test
    public void testLocalStatePersistence() throws IOException {
        final TestRunner runner = initTestRunner();

        final String bucket = runner.getProcessContext().getProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions((FlowFile) null).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3Object.KEY).evaluateAttributeExpressions(Collections.emptyMap()).getValue();
        final String cacheKey1 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key;
        final String cacheKey2 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-v2";
        final String cacheKey3 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-v3";

        /*
         * store 3 versions of state
         */
        PutS3Object.MultipartState state1orig = new PutS3Object.MultipartState();
        final PutS3Object processor = (PutS3Object) runner.getProcessor();
        processor.persistLocalState(cacheKey1, state1orig);

        PutS3Object.MultipartState state2orig = new PutS3Object.MultipartState();
        state2orig.setUploadId("1234");
        state2orig.setContentLength(1234L);
        processor.persistLocalState(cacheKey2, state2orig);

        PutS3Object.MultipartState state3orig = new PutS3Object.MultipartState();
        state3orig.setUploadId("5678");
        state3orig.setContentLength(5678L);
        processor.persistLocalState(cacheKey3, state3orig);

        final List<MultipartUpload> uploadList = new ArrayList<>();
        final MultipartUpload upload1 = MultipartUpload.builder().key(key).uploadId("").build();
        uploadList.add(upload1);
        final MultipartUpload upload2 = MultipartUpload.builder().key(key + "-v2").uploadId("1234").build();
        uploadList.add(upload2);
        final MultipartUpload upload3 = MultipartUpload.builder().key(key + "-v3").uploadId("5678").build();
        uploadList.add(upload3);
        final ListMultipartUploadsResponse response = ListMultipartUploadsResponse.builder().uploads(uploadList).build();
        final S3Client mockClient = mock(S3Client.class);
        when(mockClient.listMultipartUploads(any(ListMultipartUploadsRequest.class))).thenReturn(response);

        /*
         * reload and validate stored state
         */
        final PutS3Object.MultipartState state1new = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey1);
        assertEquals("", state1new.getUploadId());
        assertEquals(0L, state1new.getFilePosition().longValue());
        assertEquals(new ArrayList<>(), state1new.getCompletedParts());
        assertEquals(0L, state1new.getPartSize().longValue());
        assertEquals(StorageClass.fromValue(StorageClass.STANDARD.toString()), state1new.getStorageClass());
        assertEquals(0L, state1new.getContentLength().longValue());

        final PutS3Object.MultipartState state2new = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey2);
        assertEquals("1234", state2new.getUploadId());
        assertEquals(0L, state2new.getFilePosition().longValue());
        assertEquals(new ArrayList<>(), state2new.getCompletedParts());
        assertEquals(0L, state2new.getPartSize().longValue());
        assertEquals(StorageClass.fromValue(StorageClass.STANDARD.toString()), state2new.getStorageClass());
        assertEquals(1234L, state2new.getContentLength().longValue());

        final PutS3Object.MultipartState state3new = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey3);
        assertEquals("5678", state3new.getUploadId());
        assertEquals(0L, state3new.getFilePosition().longValue());
        assertEquals(new ArrayList<>(), state3new.getCompletedParts());
        assertEquals(0L, state3new.getPartSize().longValue());
        assertEquals(StorageClass.fromValue(StorageClass.STANDARD.toString()), state3new.getStorageClass());
        assertEquals(5678L, state3new.getContentLength().longValue());
    }

    @Test
    public void testStatePersistsETags() throws IOException {
        final TestRunner runner = initTestRunner();
        final PutS3Object processor = (PutS3Object) runner.getProcessor();

        final String bucket = runner.getProcessContext().getProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions((FlowFile) null).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3Object.KEY).evaluateAttributeExpressions(Collections.emptyMap()).getValue();
        final String cacheKey1 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-bv1";
        final String cacheKey2 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-bv2";
        final String cacheKey3 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-bv3";

        /*
         * store 3 versions of state
         */
        PutS3Object.MultipartState state1orig = new PutS3Object.MultipartState();
        processor.persistLocalState(cacheKey1, state1orig);

        PutS3Object.MultipartState state2orig = new PutS3Object.MultipartState();
        state2orig.setUploadId("1234");
        state2orig.setContentLength(1234L);
        processor.persistLocalState(cacheKey2, state2orig);

        PutS3Object.MultipartState state3orig = new PutS3Object.MultipartState();
        state3orig.setUploadId("5678");
        state3orig.setContentLength(5678L);
        processor.persistLocalState(cacheKey3, state3orig);

        /*
         * persist state to caches so that
         *      1. v2 has 2 and then 4 tags
         *      2. v3 has 4 and then 2 tags
         */
        state2orig.getCompletedParts().add(CompletedPart.builder().partNumber(1).eTag("state 2 tag one").build());
        state2orig.getCompletedParts().add(CompletedPart.builder().partNumber(2).eTag("state 2 tag two").build());
        processor.persistLocalState(cacheKey2, state2orig);
        state2orig.getCompletedParts().add(CompletedPart.builder().partNumber(3).eTag("state 2 tag three").build());
        state2orig.getCompletedParts().add(CompletedPart.builder().partNumber(4).eTag("state 2 tag four").build());
        processor.persistLocalState(cacheKey2, state2orig);

        state3orig.getCompletedParts().add(CompletedPart.builder().partNumber(1).eTag("state 3 tag one").build());
        state3orig.getCompletedParts().add(CompletedPart.builder().partNumber(2).eTag("state 3 tag two").build());
        state3orig.getCompletedParts().add(CompletedPart.builder().partNumber(3).eTag("state 3 tag three").build());
        state3orig.getCompletedParts().add(CompletedPart.builder().partNumber(4).eTag("state 3 tag four").build());
        processor.persistLocalState(cacheKey3, state3orig);
        state3orig.getCompletedParts().remove(state3orig.getCompletedParts().size() - 1);
        state3orig.getCompletedParts().remove(state3orig.getCompletedParts().size() - 1);
        processor.persistLocalState(cacheKey3, state3orig);

        final List<MultipartUpload> uploadList = new ArrayList<>();
        final MultipartUpload upload1 = MultipartUpload.builder().key(key + "-bv2").uploadId("1234").build();
        uploadList.add(upload1);
        final MultipartUpload upload2 = MultipartUpload.builder().key(key + "-bv3").uploadId("5678").build();
        uploadList.add(upload2);
        final ListMultipartUploadsResponse response = ListMultipartUploadsResponse.builder().uploads(uploadList).build();
        final S3Client mockClient = mock(S3Client.class);
        when(mockClient.listMultipartUploads(any(ListMultipartUploadsRequest.class))).thenReturn(response);

        /*
         * load state and validate that
         *     1. v2 restore shows 4 tags
         *     2. v3 restore shows 2 tags
         */
        final PutS3Object.MultipartState state2new = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey2);
        assertEquals("1234", state2new.getUploadId());
        assertEquals(4, state2new.getCompletedParts().size());

        final PutS3Object.MultipartState state3new = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey3);
        assertEquals("5678", state3new.getUploadId());
        assertEquals(2, state3new.getCompletedParts().size());
    }

    @Test
    public void testStateRemove() throws IOException {
        final TestRunner runner = initTestRunner();
        final PutS3Object processor = (PutS3Object) runner.getProcessor();

        final String bucket = runner.getProcessContext().getProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions((FlowFile) null).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3Object.KEY).evaluateAttributeExpressions(Collections.emptyMap()).getValue();
        final String cacheKey = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-sr";

        final List<MultipartUpload> uploadList = new ArrayList<>();
        final MultipartUpload upload1 = MultipartUpload.builder().key(key).uploadId("1234").build();
        uploadList.add(upload1);
        final ListMultipartUploadsResponse response = ListMultipartUploadsResponse.builder().uploads(uploadList).build();
        final S3Client mockClient = mock(S3Client.class);
        when(mockClient.listMultipartUploads(any(ListMultipartUploadsRequest.class))).thenReturn(response);

        /*
         * store state, retrieve and validate, remove and validate
         */
        PutS3Object.MultipartState stateOrig = new PutS3Object.MultipartState();
        stateOrig.setUploadId("1234");
        stateOrig.setContentLength(1234L);
        processor.persistLocalState(cacheKey, stateOrig);

        PutS3Object.MultipartState state1 = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey);
        assertEquals("1234", state1.getUploadId());
        assertEquals(1234L, state1.getContentLength().longValue());

        processor.persistLocalState(cacheKey, null);
        PutS3Object.MultipartState state2 = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey);
        assertNull(state2);
    }

    @Test
    public void testMultipartSmallerThanMinimum() throws IOException {
        final String FILE1_NAME = "file1";

        final byte[] megabyte = new byte[1024 * 1024];
        final Path tempFile = Files.createTempFile("s3mulitpart", "tmp");
        final FileOutputStream tempOut = new FileOutputStream(tempFile.toFile());
        long tempByteCount = 0;
        for (int i = 0; i < 5; i++) {
            tempOut.write(megabyte);
            tempByteCount += megabyte.length;
        }
        tempOut.close();
        logger.info("file size: {}", tempByteCount);
        assertTrue(tempByteCount < S3_MINIMUM_PART_SIZE);

        assertTrue(megabyte.length < S3_MINIMUM_PART_SIZE);
        assertTrue(TEST_PARTSIZE_LONG >= S3_MINIMUM_PART_SIZE && TEST_PARTSIZE_LONG <= S3_MAXIMUM_OBJECT_SIZE);

        final TestRunner runner = initTestRunner();

        runner.setProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), FILE1_NAME);
        runner.enqueue(new FileInputStream(tempFile.toFile()), attributes);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, successFiles.size());
        final List<MockFlowFile> failureFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_FAILURE);
        assertEquals(0, failureFiles.size());
        MockFlowFile ff1 = successFiles.get(0);
        assertEquals(PutS3Object.S3_API_METHOD_PUTOBJECT, ff1.getAttribute(PutS3Object.S3_API_METHOD_ATTR_KEY));
        assertEquals(FILE1_NAME, ff1.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(BUCKET_NAME, ff1.getAttribute(PutS3Object.S3_BUCKET_KEY));
        assertEquals(FILE1_NAME, ff1.getAttribute(PutS3Object.S3_OBJECT_KEY));
        assertTrue(reS3ETag.matcher(ff1.getAttribute(PutS3Object.S3_ETAG_ATTR_KEY)).matches());
        assertEquals(tempByteCount, ff1.getSize());
    }

    @Test
    public void testMultipartBetweenMinimumAndMaximum() throws IOException, InitializationException {
        final String FILE1_NAME = "file1";

        final byte[] megabyte = new byte[1024 * 1024];
        final Path tempFile = Files.createTempFile("s3mulitpart", "tmp");
        final FileOutputStream tempOut = new FileOutputStream(tempFile.toFile());
        long tempByteCount = 0;
        while (tempByteCount < TEST_PARTSIZE_LONG + 1) {
            tempOut.write(megabyte);
            tempByteCount += megabyte.length;
        }
        tempOut.close();
        logger.info("file size: {}", tempByteCount);
        assertTrue(tempByteCount > S3_MINIMUM_PART_SIZE && tempByteCount < S3_MAXIMUM_OBJECT_SIZE);
        assertTrue(tempByteCount > TEST_PARTSIZE_LONG);

        final TestRunner runner = initTestRunner();

        setSecureProperties(runner);
        runner.setProperty(RegionUtil.REGION, getRegion());
        runner.setProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        runner.setProperty(PutS3Object.MULTIPART_THRESHOLD, TEST_PARTSIZE_STRING);
        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), FILE1_NAME);
        runner.enqueue(new FileInputStream(tempFile.toFile()), attributes);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, successFiles.size());
        final List<MockFlowFile> failureFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_FAILURE);
        assertEquals(0, failureFiles.size());
        MockFlowFile ff1 = successFiles.get(0);
        assertEquals(PutS3Object.S3_API_METHOD_MULTIPARTUPLOAD, ff1.getAttribute(PutS3Object.S3_API_METHOD_ATTR_KEY));
        assertEquals(FILE1_NAME, ff1.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(BUCKET_NAME, ff1.getAttribute(PutS3Object.S3_BUCKET_KEY));
        assertEquals(FILE1_NAME, ff1.getAttribute(PutS3Object.S3_OBJECT_KEY));
        assertTrue(reS3ETag.matcher(ff1.getAttribute(PutS3Object.S3_ETAG_ATTR_KEY)).matches());
        assertEquals(tempByteCount, ff1.getSize());
    }


    @Test
    public void testObjectTags() throws IOException {
        TestRunner runner = initTestRunner();

        runner.setProperty(PutS3Object.OBJECT_TAGS_PREFIX, "tagS3");
        runner.setProperty(PutS3Object.REMOVE_TAG_PREFIX, "true");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "tag-test.txt");
        attrs.put("tagS3PII", "true");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);

        final GetObjectTaggingRequest request = GetObjectTaggingRequest.builder()
            .bucket(BUCKET_NAME)
            .key("tag-test.txt")
            .build();
        GetObjectTaggingResponse response = getClient().getObjectTagging(request);
        List<Tag> objectTags = response.tagSet();

        for (Tag tag : objectTags) {
            logger.info("Tag Key : {}, Tag Value : {}", tag.key(), tag.value());
        }

        assertEquals(1, objectTags.size());
        assertEquals("PII", objectTags.get(0).key());
        assertEquals("true", objectTags.get(0).value());
    }

    @Test
    public void testEncryptionServiceWithServerSideS3EncryptionStrategyUsingSingleUpload() throws IOException, InitializationException {
        byte[] smallData = Files.readAllBytes(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));
        testEncryptionServiceWithServerSideS3EncryptionStrategy(smallData);
    }

    @Test
    public void testEncryptionServiceWithServerSideS3EncryptionStrategyUsingMultipartUpload() throws IOException, InitializationException {
        byte[] largeData = new byte[51 * 1024 * 1024];
        testEncryptionServiceWithServerSideS3EncryptionStrategy(largeData);
    }

    private void testEncryptionServiceWithServerSideS3EncryptionStrategy(byte[] data) throws IOException, InitializationException {
        TestRunner runner = createPutEncryptionTestRunner(AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3, null, null);

        Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test.txt");
        runner.enqueue(data, attrs);
        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        assertEquals(0, runner.getFlowFilesForRelationship(PutS3Object.REL_FAILURE).size());
        MockFlowFile putSuccess = flowFiles.get(0);
        assertEquals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3, putSuccess.getAttribute(PutS3Object.S3_ENCRYPTION_STRATEGY));

        MockFlowFile flowFile = fetchEncryptedFlowFile(attrs, AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3, null, null);
        flowFile.assertContentEquals(data);
        flowFile.assertAttributeEquals(PutS3Object.S3_SSE_ALGORITHM, "AES256");
        flowFile.assertAttributeEquals(PutS3Object.S3_ENCRYPTION_STRATEGY, AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3);
    }

    @Test
    public void testEncryptionServiceWithServerSideKMSEncryptionStrategyUsingSingleUpload() throws IOException, InitializationException {
        byte[] smallData = Files.readAllBytes(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));
        testEncryptionServiceWithServerSideKMSEncryptionStrategy(smallData);
    }

    @Test
    public void testEncryptionServiceWithServerSideKMSEncryptionStrategyUsingMultipartUpload() throws IOException, InitializationException {
        byte[] largeData = new byte[51 * 1024 * 1024];
        testEncryptionServiceWithServerSideKMSEncryptionStrategy(largeData);
    }

    private void testEncryptionServiceWithServerSideKMSEncryptionStrategy(byte[] data) throws IOException, InitializationException {
        TestRunner runner = createPutEncryptionTestRunner(AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS, kmsKeyId, null);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test.txt");
        runner.enqueue(data, attrs);
        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        assertEquals(0, runner.getFlowFilesForRelationship(PutS3Object.REL_FAILURE).size());
        MockFlowFile putSuccess = flowFiles.get(0);
        assertEquals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS, putSuccess.getAttribute(PutS3Object.S3_ENCRYPTION_STRATEGY));

        MockFlowFile flowFile = fetchEncryptedFlowFile(attrs, AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS, kmsKeyId, null);
        flowFile.assertContentEquals(data);
        flowFile.assertAttributeEquals(PutS3Object.S3_SSE_ALGORITHM, "aws:kms");
        flowFile.assertAttributeEquals(PutS3Object.S3_ENCRYPTION_STRATEGY, AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS);
    }

    @Test
    public void testEncryptionServiceWithServerSideCEncryptionStrategyUsingSingleUpload() throws IOException, InitializationException {
        byte[] smallData = Files.readAllBytes(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));
        testEncryptionServiceWithServerSideCEncryptionStrategy(smallData);
    }

    @Test
    public void testEncryptionServiceWithServerSideCEncryptionStrategyUsingMultipartUpload() throws IOException, InitializationException {
        byte[] largeData = new byte[51 * 1024 * 1024];
        testEncryptionServiceWithServerSideCEncryptionStrategy(largeData);
    }

    private void testEncryptionServiceWithServerSideCEncryptionStrategy(byte[] data) throws IOException, InitializationException {
        TestRunner runner = createPutEncryptionTestRunner(AmazonS3EncryptionService.STRATEGY_NAME_SSE_C, null, randomKeyMaterial);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test.txt");
        runner.enqueue(data, attrs);
        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        assertEquals(0, runner.getFlowFilesForRelationship(PutS3Object.REL_FAILURE).size());
        MockFlowFile putSuccess = flowFiles.get(0);
        assertEquals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_C, putSuccess.getAttribute(PutS3Object.S3_ENCRYPTION_STRATEGY));

        MockFlowFile flowFile = fetchEncryptedFlowFile(attrs, AmazonS3EncryptionService.STRATEGY_NAME_SSE_C, null, randomKeyMaterial);
        flowFile.assertContentEquals(data);
        // successful fetch does not indicate type of original encryption:
        flowFile.assertAttributeEquals(PutS3Object.S3_SSE_ALGORITHM, null);
        // but it does indicate it via our specific attribute:
        flowFile.assertAttributeEquals(PutS3Object.S3_ENCRYPTION_STRATEGY, AmazonS3EncryptionService.STRATEGY_NAME_SSE_C);
    }

    @Test
    public void testEncryptionServiceWithClientSideCEncryptionStrategyUsingSingleUpload() throws InitializationException, IOException {
        byte[] smallData = Files.readAllBytes(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));
        testEncryptionServiceWithClientSideCEncryptionStrategy(smallData);
    }

    @Test
    public void testEncryptionServiceWithClientSideCEncryptionStrategyUsingMultipartUpload() throws IOException, InitializationException {
        byte[] largeData = new byte[51 * 1024 * 1024];
        testEncryptionServiceWithClientSideCEncryptionStrategy(largeData);
    }

    private void testEncryptionServiceWithClientSideCEncryptionStrategy(byte[] data) throws InitializationException, IOException {
        TestRunner runner = createPutEncryptionTestRunner(AmazonS3EncryptionService.STRATEGY_NAME_CSE_C, null, randomKeyMaterial);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test.txt");
        runner.enqueue(data, attrs);
        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        assertEquals(0, runner.getFlowFilesForRelationship(PutS3Object.REL_FAILURE).size());
        MockFlowFile putSuccess = flowFiles.get(0);
        assertEquals(AmazonS3EncryptionService.STRATEGY_NAME_CSE_C, putSuccess.getAttribute(PutS3Object.S3_ENCRYPTION_STRATEGY));

        MockFlowFile flowFile = fetchEncryptedFlowFile(attrs, AmazonS3EncryptionService.STRATEGY_NAME_CSE_C, null, randomKeyMaterial);
        flowFile.assertAttributeEquals(PutS3Object.S3_ENCRYPTION_STRATEGY, AmazonS3EncryptionService.STRATEGY_NAME_CSE_C);
        flowFile.assertContentEquals(data);
    }

    private TestRunner createPutEncryptionTestRunner(String strategyName, String keyId, String keyMaterial) throws InitializationException {
        TestRunner runner = createEncryptionTestRunner(PutS3Object.class, strategyName, keyId, keyMaterial);

        runner.setProperty(PutS3Object.MULTIPART_THRESHOLD, "50 MB");
        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, "50 MB");

        return runner;
    }

    private TestRunner createFetchEncryptionTestRunner(String strategyName, String keyId, String keyMaterial) throws InitializationException {
        if (strategyName.equals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3) || strategyName.equals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS)) {
            strategyName = null;
        }

        return createEncryptionTestRunner(FetchS3Object.class, strategyName, keyId, keyMaterial);
    }

    private MockFlowFile fetchEncryptedFlowFile(Map<String, String> attributes, String strategyName, String keyId, String keyMaterial) throws InitializationException {
        final TestRunner runner = createFetchEncryptionTestRunner(strategyName, keyId, keyMaterial);
        runner.enqueue(new byte[0], attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        return flowFiles.get(0);
    }

    private TestRunner createEncryptionTestRunner(final Class<? extends AbstractS3Processor> processorClass, String strategyName, String keyId, String keyMaterial) throws InitializationException {
        final TestRunner runner = initRunner(processorClass);

        if (strategyName != null) {
            final StandardS3EncryptionService service = new StandardS3EncryptionService();
            runner.addControllerService(PutS3Object.ENCRYPTION_SERVICE.getName(), service);
            runner.setProperty(PutS3Object.ENCRYPTION_SERVICE, service.getIdentifier());

            runner.setProperty(service, StandardS3EncryptionService.ENCRYPTION_STRATEGY, strategyName);
            runner.setProperty(service, StandardS3EncryptionService.KMS_KEY_ID, keyId);
            runner.setProperty(service, StandardS3EncryptionService.KEY_MATERIAL, keyMaterial);

            runner.enableControllerService(service);
        }

        return runner;
    }


    @Test
    public void testChunkedEncodingDisabled() throws IOException {
        TestRunner runner = initTestRunner();

        runner.setProperty(PutS3Object.USE_CHUNKED_ENCODING, "false");

        executeSimplePutTest(runner);
    }

    @Test
    public void testPathStyleAccessEnabled() throws IOException {
        TestRunner runner = initTestRunner();

        runner.setProperty(PutS3Object.USE_PATH_STYLE_ACCESS, "true");

        executeSimplePutTest(runner);
    }

    private TestRunner initFetchRunner() {
        return initRunner(FetchS3Object.class);
    }

    private TestRunner initTestRunner() {
        return initRunner(PutS3Object.class);
    }

    private void executeSimplePutTest(TestRunner runner) throws IOException {
        runner.assertValid();

        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }
}
