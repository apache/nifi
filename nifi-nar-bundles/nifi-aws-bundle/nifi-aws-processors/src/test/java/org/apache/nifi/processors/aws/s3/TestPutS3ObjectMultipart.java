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

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestPutS3ObjectMultipart {

    ProcessContext context;
    TestRunner runner;
    PutS3ObjectMultipart processor;

    final String TEST_CREDFILE = "dummyCreds.txt";
    final String TEST_ENDPOINT = "https://endpoint.com";
    final String TEST_BUCKET = "bucket";
    final String TEST_TRANSIT_URI = "https://bucket.endpoint.com";
    final String TEST_KEY = "key";
    final String TEST_PARTSIZE = "50 mb";
    final Long   TEST_PARTSIZE_LONG = 50L * 1024L * 1024L;
    final String LINESEP = System.getProperty("line.separator");

    private void createDummyCreds() throws IOException {
        File dummyCreds = new File(TEST_CREDFILE);
        if (dummyCreds.exists()) {
            assertTrue(dummyCreds.delete());
        }
        assertTrue(dummyCreds.createNewFile());
        FileOutputStream fos = new FileOutputStream(dummyCreds);
        fos.write(("accessKey=access" + LINESEP + "secretKey=secret" + LINESEP).getBytes());
        fos.close();
    }

    @Before
    public void before() throws InitializationException, IOException {
        createDummyCreds();

        processor = new TestablePutS3ObjectMultipart();
        runner = TestRunners.newTestRunner(processor);
        context = runner.getProcessContext();

        runner.setProperty(PutS3ObjectMultipart.BUCKET, TEST_BUCKET);
        runner.setProperty(PutS3ObjectMultipart.KEY, TEST_KEY);
        runner.setProperty(PutS3ObjectMultipart.ENDPOINT_OVERRIDE, TEST_ENDPOINT);
        runner.setProperty(PutS3ObjectMultipart.CREDENTIALS_FILE, TEST_CREDFILE);
        runner.setProperty(PutS3ObjectMultipart.PART_SIZE, TEST_PARTSIZE);
    }

    @After
    public void after() {
        File[] stateFiles = new File(PutS3ObjectMultipart.PERSISTENCE_ROOT).listFiles();
        if (stateFiles != null) {
            for (File f : stateFiles) {
                assertTrue(f.delete());
            }
        }
        File credfile = new File(TEST_CREDFILE);
        if (credfile.exists()) {
            assertTrue(credfile.delete());
        }
    }

    @Test
    public void testStateDefaults() {
        PutS3ObjectMultipart.MultipartState state1 = new PutS3ObjectMultipart.MultipartState();
        assertEquals(state1.uploadId, "");
        assertEquals(state1.filePosition, (Long) 0L);
        assertEquals(state1.partETags.size(), 0L);
        assertEquals(state1.uploadETag, "");
        assertEquals(state1.partSize, (Long) 0L);
        assertEquals(state1.storageClass.toString(), StorageClass.Standard.toString());
        assertEquals(state1.contentLength, (Long) 0L);
    }

    @Test
    public void testStateToString() {
        final String target = "UID-test1234567890#10001#1/PartETag-1,2/PartETag-2,3/PartETag-3,4/PartETag-4#UploadETag-test0987654321#20002#REDUCED_REDUNDANCY#30003";
        PutS3ObjectMultipart.MultipartState state2 = new PutS3ObjectMultipart.MultipartState();
        state2.setUploadId("UID-test1234567890");
        state2.setFilePosition(10001L);
        for (Integer partNum = 1; partNum < 5; partNum++) {
            state2.addPartETag(new PartETag(partNum, "PartETag-" + partNum.toString()));
        }
        state2.setUploadETag("UploadETag-test0987654321");
        state2.setPartSize(20002L);
        state2.setStorageClass(StorageClass.ReducedRedundancy);
        state2.setContentLength(30003L);
        assertEquals(target, state2.toString());
    }

    @Test
    public void testProperties() throws IOException {
//        runner.setProperty(PutS3ObjectMultipart.BUCKET, "anonymous-test-bucket-00000000");
//        runner.setProperty(PutS3ObjectMultipart.KEY, "folder/1.txt");
//        runner.setProperty(PutS3ObjectMultipart.ENDPOINT_OVERRIDE, "https://endpoint.com");
//        runner.setProperty(PutS3ObjectMultipart.ACCESS_KEY, "access");
//        runner.setProperty(PutS3ObjectMultipart.SECRET_KEY, "secret");
//        runner.setProperty(PutS3ObjectMultipart.CREDENTIALS_FILE, "creds");
//        runner.setProperty(PutS3ObjectMultipart.PART_SIZE, "50 mb");

        assertEquals(TEST_BUCKET, context.getProperty(PutS3ObjectMultipart.BUCKET).toString());
        assertEquals(TEST_KEY, context.getProperty(PutS3ObjectMultipart.KEY).toString());
        assertEquals(TEST_ENDPOINT, context.getProperty(PutS3ObjectMultipart.ENDPOINT_OVERRIDE).toString());
//        assertNull(context.getProperty(PutS3ObjectMultipart.ACCESS_KEY).toString());
//        assertNull(context.getProperty(PutS3ObjectMultipart.SECRET_KEY).toString());
        assertEquals(TEST_CREDFILE, context.getProperty(PutS3ObjectMultipart.CREDENTIALS_FILE).toString());
        assertEquals(TEST_PARTSIZE_LONG.longValue(),
                context.getProperty(PutS3ObjectMultipart.PART_SIZE).asDataSize(DataUnit.B).longValue());
    }

    @Test
    public void testPersistence() throws IOException {
        final String bucket = runner.getProcessContext().getProperty(PutS3ObjectMultipart.BUCKET).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3ObjectMultipart.KEY).getValue();
        final String cacheKey1 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key;
        final String cacheKey2 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-v2";
        final String cacheKey3 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-v3";

        /*
         * store 3 versions of state
         */
        PutS3ObjectMultipart.MultipartState state1orig = new PutS3ObjectMultipart.MultipartState();
        processor.persistState(cacheKey1, state1orig);

        PutS3ObjectMultipart.MultipartState state2orig = new PutS3ObjectMultipart.MultipartState();
        state2orig.uploadId = "1234";
        state2orig.setContentLength(1234L);
        processor.persistState(cacheKey2, state2orig);

        PutS3ObjectMultipart.MultipartState state3orig = new PutS3ObjectMultipart.MultipartState();
        state3orig.uploadId = "5678";
        state3orig.setContentLength(5678L);
        processor.persistState(cacheKey3, state3orig);

        /*
         * reload and validate stored state
         */
        final PutS3ObjectMultipart.MultipartState state1new = processor.getState(cacheKey1);
        assertEquals("", state1new.uploadId);
        assertEquals(0L, state1new.filePosition.longValue());
        assertEquals(new ArrayList<PartETag>(), state1new.partETags);
        assertEquals("", state1new.uploadETag);
        assertEquals(0L, state1new.partSize.longValue());
        assertEquals(StorageClass.fromValue(StorageClass.Standard.toString()), state1new.storageClass);
        assertEquals(0L, state1new.contentLength.longValue());

        final PutS3ObjectMultipart.MultipartState state2new = processor.getState(cacheKey2);
        assertEquals("1234", state2new.uploadId);
        assertEquals(0L, state2new.filePosition.longValue());
        assertEquals(new ArrayList<PartETag>(), state2new.partETags);
        assertEquals("", state2new.uploadETag);
        assertEquals(0L, state2new.partSize.longValue());
        assertEquals(StorageClass.fromValue(StorageClass.Standard.toString()), state2new.storageClass);
        assertEquals(1234L, state2new.contentLength.longValue());

        final PutS3ObjectMultipart.MultipartState state3new = processor.getState(cacheKey3);
        assertEquals("5678", state3new.uploadId);
        assertEquals(0L, state3new.filePosition.longValue());
        assertEquals(new ArrayList<PartETag>(), state3new.partETags);
        assertEquals("", state3new.uploadETag);
        assertEquals(0L, state3new.partSize.longValue());
        assertEquals(StorageClass.fromValue(StorageClass.Standard.toString()), state3new.storageClass);
        assertEquals(5678L, state3new.contentLength.longValue());
    }

    @Test
    public void testStatePersistsETags() throws IOException {
        final String bucket = runner.getProcessContext().getProperty(PutS3ObjectMultipart.BUCKET).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3ObjectMultipart.KEY).getValue();
        final String cacheKey1 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-bv1";
        final String cacheKey2 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-bv2";
        final String cacheKey3 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-bv3";

        /*
         * store 3 versions of state
         */
        PutS3ObjectMultipart.MultipartState state1orig = new PutS3ObjectMultipart.MultipartState();
        processor.persistState(cacheKey1, state1orig);

        PutS3ObjectMultipart.MultipartState state2orig = new PutS3ObjectMultipart.MultipartState();
        state2orig.uploadId = "1234";
        state2orig.setContentLength(1234L);
        processor.persistState(cacheKey2, state2orig);

        PutS3ObjectMultipart.MultipartState state3orig = new PutS3ObjectMultipart.MultipartState();
        state3orig.uploadId = "5678";
        state3orig.setContentLength(5678L);
        processor.persistState(cacheKey3, state3orig);

        /*
         * persist state to caches so that
         *      1. v2 has 2 and then 4 tags
         *      2. v3 has 4 and then 2 tags
         */
        state2orig.partETags.add(new PartETag(1, "state 2 tag one"));
        state2orig.partETags.add(new PartETag(2, "state 2 tag two"));
        processor.persistState(cacheKey2, state2orig);
        state2orig.partETags.add(new PartETag(3, "state 2 tag three"));
        state2orig.partETags.add(new PartETag(4, "state 2 tag four"));
        processor.persistState(cacheKey2, state2orig);

        state3orig.partETags.add(new PartETag(1, "state 3 tag one"));
        state3orig.partETags.add(new PartETag(2, "state 3 tag two"));
        state3orig.partETags.add(new PartETag(3, "state 3 tag three"));
        state3orig.partETags.add(new PartETag(4, "state 3 tag four"));
        processor.persistState(cacheKey3, state3orig);
        state3orig.partETags.remove(state3orig.partETags.size() - 1);
        state3orig.partETags.remove(state3orig.partETags.size() - 1);
        processor.persistState(cacheKey3, state3orig);

        /*
         * load state and validate that
         *     1. v2 restore shows 4 tags
         *     2. v3 restore shows 2 tags
         */
        final PutS3ObjectMultipart.MultipartState state2new = processor.getState(cacheKey2);
        assertEquals("1234", state2new.uploadId);
        assertEquals(4, state2new.partETags.size());

        final PutS3ObjectMultipart.MultipartState state3new = processor.getState(cacheKey3);
        assertEquals("5678", state3new.uploadId);
        assertEquals(2, state3new.partETags.size());
    }

    @Test
    public void testStateRemove() throws IOException {
        final String bucket = runner.getProcessContext().getProperty(PutS3ObjectMultipart.BUCKET).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3ObjectMultipart.KEY).getValue();
        final String cacheKey = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-sr";

        /*
         * store state, retrieve and validate, remove and validate
         */
        PutS3ObjectMultipart.MultipartState stateOrig = new PutS3ObjectMultipart.MultipartState();
        stateOrig.uploadId = "1234";
        stateOrig.setContentLength(1234L);
        processor.persistState(cacheKey, stateOrig);

        PutS3ObjectMultipart.MultipartState state1 = processor.getState(cacheKey);
        assertEquals("1234", state1.uploadId);
        assertEquals(1234L, state1.contentLength.longValue());

        processor.persistState(cacheKey, null);
        PutS3ObjectMultipart.MultipartState state2 = processor.getState(cacheKey);
        assertNull(state2);
    }

    @Test
    public void testApiInteractions() throws IOException {
        final String FILE1_NAME = "file1";
        final String ALPHA_LF = "abcdefghijklmnopqrstuvwxyz\n";
        final String ALPHA_6x = ALPHA_LF + ALPHA_LF + ALPHA_LF + ALPHA_LF + ALPHA_LF + ALPHA_LF;
        final String FILE1_CONTENTS = ALPHA_6x + ALPHA_6x + ALPHA_6x + ALPHA_6x + ALPHA_6x + ALPHA_6x;

        runner.setProperty(PutS3ObjectMultipart.BUCKET, TEST_BUCKET);
        runner.setProperty(PutS3ObjectMultipart.KEY, TEST_KEY);
        runner.setProperty(PutS3ObjectMultipart.ENDPOINT_OVERRIDE, TEST_ENDPOINT);
        runner.setProperty(PutS3ObjectMultipart.CREDENTIALS_FILE, TEST_CREDFILE);
        runner.setProperty(PutS3ObjectMultipart.PART_SIZE, TEST_PARTSIZE);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), FILE1_NAME);
        runner.enqueue(FILE1_CONTENTS.getBytes(), attributes);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3ObjectMultipart.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3ObjectMultipart.REL_SUCCESS);
        assertEquals(1, successFiles.size());
        final List<MockFlowFile> failureFiles = runner.getFlowFilesForRelationship(PutS3ObjectMultipart.REL_FAILURE);
        assertEquals(0, failureFiles.size());
        MockFlowFile ff1 = successFiles.get(0);
        assertEquals(FILE1_NAME, ff1.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(TEST_BUCKET, ff1.getAttribute(PutS3ObjectMultipart.S3_BUCKET_KEY));
        assertEquals(TEST_KEY, ff1.getAttribute(PutS3ObjectMultipart.S3_OBJECT_KEY));
        assertEquals(((TestablePutS3ObjectMultipart)processor).MOCK_CLIENT_UPLOADID,
                ff1.getAttribute(PutS3ObjectMultipart.S3_UPLOAD_ID_ATTR_KEY));
        assertEquals(((TestablePutS3ObjectMultipart)processor).MOCK_CLIENT_FILE_ETAG_PREFIX +
                ((TestablePutS3ObjectMultipart)processor).MOCK_CLIENT_UPLOADID,
                ff1.getAttribute(PutS3ObjectMultipart.S3_ETAG_ATTR_KEY));
        assertEquals(((TestablePutS3ObjectMultipart)processor).MOCK_CLIENT_VERSION,
                ff1.getAttribute(PutS3ObjectMultipart.S3_VERSION_ATTR_KEY));
    }

    @Test
    public void testDynamicProperty() throws IOException {
        final String DYNAMIC_ATTRIB_KEY = "fs.runTimestamp";
        final String DYNAMIC_ATTRIB_VALUE = "${now():toNumber()}";

        runner.setProperty(PutS3ObjectMultipart.BUCKET, TEST_BUCKET);
        runner.setProperty(PutS3ObjectMultipart.KEY, TEST_KEY);
        runner.setProperty(PutS3ObjectMultipart.ENDPOINT_OVERRIDE, TEST_ENDPOINT);
        runner.setProperty(PutS3ObjectMultipart.CREDENTIALS_FILE, TEST_CREDFILE);
        runner.setProperty(PutS3ObjectMultipart.PART_SIZE, TEST_PARTSIZE);
        PropertyDescriptor testAttrib = processor.getSupportedDynamicPropertyDescriptor(DYNAMIC_ATTRIB_KEY);
        runner.setProperty(testAttrib, DYNAMIC_ATTRIB_VALUE);

        final String FILE1_NAME = "file1";
        Map<String, String> attribs = new HashMap<>();
        attribs.put(CoreAttributes.FILENAME.key(), FILE1_NAME);
        runner.enqueue("123".getBytes(), attribs);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3ObjectMultipart.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3ObjectMultipart.REL_SUCCESS);
        assertEquals(1, successFiles.size());
        MockFlowFile ff1 = successFiles.get(0);

        Long now = System.currentTimeMillis();
        String millisNow = Long.toString(now);
        String millisOneSecAgo = Long.toString(now - 1000L);
        String usermeta = ff1.getAttribute(PutS3ObjectMultipart.S3_USERMETA_ATTR_KEY);
        String[] usermetaLine0 = usermeta.split(LINESEP)[0].split("=");
        String usermetaKey0 = usermetaLine0[0];
        String usermetaValue0 = usermetaLine0[1];
        assertEquals(DYNAMIC_ATTRIB_KEY, usermetaKey0);
        assertTrue(usermetaValue0.compareTo(millisOneSecAgo) >=0 && usermetaValue0.compareTo(millisNow) <= 0);
    }

    @Test
    public void testProvenance() throws InitializationException {
        final String PROV1_FILE = "provfile1";

        Map<String, String> attribs = new HashMap<>();
        attribs.put(CoreAttributes.FILENAME.key(), PROV1_FILE);
        runner.enqueue("prov1 contents".getBytes(), attribs);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3ObjectMultipart.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3ObjectMultipart.REL_SUCCESS);
        assertEquals(1, successFiles.size());

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        ProvenanceEventRecord provRec1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, provRec1.getEventType());
        assertEquals(processor.getIdentifier(), provRec1.getComponentId());
        assertEquals(TEST_TRANSIT_URI + "/" + TEST_KEY, provRec1.getTransitUri());
        assertEquals(9, provRec1.getUpdatedAttributes().size());
    }

    public class TestablePutS3ObjectMultipart extends PutS3ObjectMultipart {
        public final String MOCK_CLIENT_UPLOADID = UUID.nameUUIDFromBytes("UPLOADID".getBytes()).toString();
        public final String MOCK_CLIENT_PART_ETAG_PREFIX = UUID.nameUUIDFromBytes("PARTETAG".getBytes()).toString();
        public final String MOCK_CLIENT_FILE_ETAG_PREFIX = UUID.nameUUIDFromBytes("FILEETAG".getBytes()).toString();
        public final String MOCK_CLIENT_VERSION = "mock-version";

        @Override
        protected AmazonS3Client createClient(ProcessContext context, AWSCredentials credentials,
                                              ClientConfiguration config) {
            return new MockAWSClient(MOCK_CLIENT_UPLOADID, MOCK_CLIENT_PART_ETAG_PREFIX, MOCK_CLIENT_FILE_ETAG_PREFIX,
                    MOCK_CLIENT_VERSION);
        }
    }

    private class MockAWSClient extends AmazonS3Client {
        private String uploadId;
        private String partETag;
        private String fileETag;
        private String version;

        public MockAWSClient(String uploadId, String partEtag, String fileEtag, String version) {
            this.uploadId = uploadId;
            this.partETag = partEtag;
            this.fileETag = fileEtag;
            this.version = version;
        }

        @Override
        public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest initiateMultipartUploadRequest) throws AmazonClientException {
            InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
            result.setBucketName(initiateMultipartUploadRequest.getBucketName());
            result.setKey(initiateMultipartUploadRequest.getKey());
            result.setUploadId(uploadId);
            return result;
        }

        @Override
        public UploadPartResult uploadPart(UploadPartRequest uploadPartRequest) throws AmazonClientException {
            UploadPartResult result = new UploadPartResult();
            result.setPartNumber(uploadPartRequest.getPartNumber());
            result.setETag(partETag + Integer.toString(result.getPartNumber()) + "-" + uploadPartRequest.getUploadId());
            return result;
        }

        @Override
        public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest completeMultipartUploadRequest) throws AmazonClientException {
            CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
            result.setBucketName(completeMultipartUploadRequest.getBucketName());
            result.setKey(completeMultipartUploadRequest.getKey());
            result.setETag(fileETag + completeMultipartUploadRequest.getUploadId());
            Date date1 = new Date();
            date1.setTime(System.currentTimeMillis());
            result.setExpirationTime(date1);
            result.setVersionId(version);
            return result;
        }
    }
}
