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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.sun.org.apache.xerces.internal.impl.xpath.regex.RegularExpression;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.services.s3.model.StorageClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured")
public class TestPutS3Object extends AbstractS3Test {

    final PutS3Object processor = new TestablePutS3Object();
    final TestRunner runner = TestRunners.newTestRunner(processor);
    final ProcessContext context = runner.getProcessContext();

    final static String TEST_ENDPOINT = "https://endpoint.com";
    final static String TEST_TRANSIT_URI = "https://" + BUCKET_NAME + "/bucket.endpoint.com";
    final static String TEST_PARTSIZE_STRING = "50 mb";
    final static Long   TEST_PARTSIZE_LONG = 50L * 1024L * 1024L;
    final static String LINESEP = System.getProperty("line.separator");

    final RegularExpression reS3ETag = new RegularExpression("[0-9a-fA-f]{32,32}");

    @Before
    public void before() {
        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.KEY, SAMPLE_FILE_RESOURCE_NAME);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
    }

    @Test
    public void testSimplePut() throws IOException {
        assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        for (int i = 0; i < 3; i++) {
            final Map<String, String> attrs = new HashMap<>();
            attrs.put("filename", String.valueOf(i) + ".txt");
            runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        }
        runner.run(3);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 3);
    }

    @Test
    public void testPutInFolder() throws IOException {
        runner.setProperty(PutS3Object.REGION, REGION);

        assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/1.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testStorageClass() throws IOException {
        runner.setProperty(PutS3Object.KEY, "${filename}");

        int bytesNeeded = 55 * 1024 * 1024;
        StringBuilder bldr = new StringBuilder(bytesNeeded + 1000);
        for (int line = 0; line < 55; line++) {
            bldr.append(String.format("line %06d This is sixty-three characters plus the EOL marker!\n", line));
        }
        String data55mb = bldr.toString();

        runner.setProperty(PutS3Object.STORAGE_CLASS, StorageClass.ReducedRedundancy.name());

        assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/2.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        attrs.put("filename", "folder/3.txt");
        runner.enqueue(data55mb.getBytes(), attrs);

        runner.run(2);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 2);
        FlowFile file1 = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS).get(0);
        assertEquals(StorageClass.ReducedRedundancy.toString(),
                file1.getAttribute(PutS3Object.S3_STORAGECLASS_ATTR_KEY));
        FlowFile file2 = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS).get(1);
        assertEquals(StorageClass.ReducedRedundancy.toString(),
                file2.getAttribute(PutS3Object.S3_STORAGECLASS_ATTR_KEY));
    }

    @Test
    public void testPermissions() throws IOException {
        runner.setProperty(PutS3Object.FULL_CONTROL_USER_LIST,"28545acd76c35c7e91f8409b95fd1aa0c0914bfa1ac60975d9f48bc3c5e090b5");
        runner.setProperty(PutS3Object.REGION, REGION);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/4.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testStateDefaults() {
        PutS3Object.MultipartState state1 = new PutS3Object.MultipartState();
        assertEquals(state1.getUploadId(), "");
        assertEquals(state1.getFilePosition(), (Long) 0L);
        assertEquals(state1.getPartETags().size(), 0L);
        assertEquals(state1.getPartSize(), (Long) 0L);
        assertEquals(state1.getStorageClass().toString(), StorageClass.Standard.toString());
        assertEquals(state1.getContentLength(), (Long) 0L);
    }

    @Test
    public void testStateToString() throws IOException, InitializationException {
        final String target = "UID-test1234567890#10001#1/PartETag-1,2/PartETag-2,3/PartETag-3,4/PartETag-4#20002#REDUCED_REDUNDANCY#30003";
        PutS3Object.MultipartState state2 = new PutS3Object.MultipartState();
        state2.setUploadId("UID-test1234567890");
        state2.setFilePosition(10001L);
        for (Integer partNum = 1; partNum < 5; partNum++) {
            state2.addPartETag(new PartETag(partNum, "PartETag-" + partNum.toString()));
        }
        state2.setPartSize(20002L);
        state2.setStorageClass(StorageClass.ReducedRedundancy);
        state2.setContentLength(30003L);
        assertEquals(target, state2.toString());
    }

    @Test
    public void testProperties() throws IOException {
        runner.setProperty(PutS3Object.FULL_CONTROL_USER_LIST,
                "28545acd76c35c7e91f8409b95fd1aa0c0914bfa1ac60975d9f48bc3c5e090b5");
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.ENDPOINT_OVERRIDE, TEST_ENDPOINT);
        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);


        assertEquals(BUCKET_NAME, context.getProperty(PutS3Object.BUCKET).toString());
        assertEquals(SAMPLE_FILE_RESOURCE_NAME, context.getProperty(PutS3Object.KEY).evaluateAttributeExpressions().toString());
        assertEquals(TEST_ENDPOINT, context.getProperty(PutS3Object.ENDPOINT_OVERRIDE).toString());
        assertEquals(CREDENTIALS_FILE, context.getProperty(PutS3Object.CREDENTIALS_FILE).toString());
        assertEquals(TEST_PARTSIZE_LONG.longValue(),
                context.getProperty(PutS3Object.MULTIPART_PART_SIZE).asDataSize(DataUnit.B).longValue());
    }

    @Test
    public void testPersistence() throws IOException {
        final String bucket = runner.getProcessContext().getProperty(PutS3Object.BUCKET).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3Object.KEY).getValue();
        final String cacheKey1 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key;
        final String cacheKey2 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-v2";
        final String cacheKey3 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-v3";

        /*
         * store 3 versions of state
         */
        PutS3Object.MultipartState state1orig = new PutS3Object.MultipartState();
        processor.persistState(cacheKey1, state1orig);

        PutS3Object.MultipartState state2orig = new PutS3Object.MultipartState();
        state2orig.setUploadId("1234");
        state2orig.setContentLength(1234L);
        processor.persistState(cacheKey2, state2orig);

        PutS3Object.MultipartState state3orig = new PutS3Object.MultipartState();
        state3orig.setUploadId("5678");
        state3orig.setContentLength(5678L);
        processor.persistState(cacheKey3, state3orig);

        /*
         * reload and validate stored state
         */
        final PutS3Object.MultipartState state1new = processor.getState(cacheKey1);
        assertEquals("", state1new.getUploadId());
        assertEquals(0L, state1new.getFilePosition().longValue());
        assertEquals(new ArrayList<PartETag>(), state1new.getPartETags());
        assertEquals(0L, state1new.getPartSize().longValue());
        assertEquals(StorageClass.fromValue(StorageClass.Standard.toString()), state1new.getStorageClass());
        assertEquals(0L, state1new.getContentLength().longValue());

        final PutS3Object.MultipartState state2new = processor.getState(cacheKey2);
        assertEquals("1234", state2new.getUploadId());
        assertEquals(0L, state2new.getFilePosition().longValue());
        assertEquals(new ArrayList<PartETag>(), state2new.getPartETags());
        assertEquals(0L, state2new.getPartSize().longValue());
        assertEquals(StorageClass.fromValue(StorageClass.Standard.toString()), state2new.getStorageClass());
        assertEquals(1234L, state2new.getContentLength().longValue());

        final PutS3Object.MultipartState state3new = processor.getState(cacheKey3);
        assertEquals("5678", state3new.getUploadId());
        assertEquals(0L, state3new.getFilePosition().longValue());
        assertEquals(new ArrayList<PartETag>(), state3new.getPartETags());
        assertEquals(0L, state3new.getPartSize().longValue());
        assertEquals(StorageClass.fromValue(StorageClass.Standard.toString()), state3new.getStorageClass());
        assertEquals(5678L, state3new.getContentLength().longValue());
    }

    @Test
    public void testStatePersistsETags() throws IOException {
        final String bucket = runner.getProcessContext().getProperty(PutS3Object.BUCKET).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3Object.KEY).getValue();
        final String cacheKey1 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-bv1";
        final String cacheKey2 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-bv2";
        final String cacheKey3 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-bv3";

        /*
         * store 3 versions of state
         */
        PutS3Object.MultipartState state1orig = new PutS3Object.MultipartState();
        processor.persistState(cacheKey1, state1orig);

        PutS3Object.MultipartState state2orig = new PutS3Object.MultipartState();
        state2orig.setUploadId("1234");
        state2orig.setContentLength(1234L);
        processor.persistState(cacheKey2, state2orig);

        PutS3Object.MultipartState state3orig = new PutS3Object.MultipartState();
        state3orig.setUploadId("5678");
        state3orig.setContentLength(5678L);
        processor.persistState(cacheKey3, state3orig);

        /*
         * persist state to caches so that
         *      1. v2 has 2 and then 4 tags
         *      2. v3 has 4 and then 2 tags
         */
        state2orig.getPartETags().add(new PartETag(1, "state 2 tag one"));
        state2orig.getPartETags().add(new PartETag(2, "state 2 tag two"));
        processor.persistState(cacheKey2, state2orig);
        state2orig.getPartETags().add(new PartETag(3, "state 2 tag three"));
        state2orig.getPartETags().add(new PartETag(4, "state 2 tag four"));
        processor.persistState(cacheKey2, state2orig);

        state3orig.getPartETags().add(new PartETag(1, "state 3 tag one"));
        state3orig.getPartETags().add(new PartETag(2, "state 3 tag two"));
        state3orig.getPartETags().add(new PartETag(3, "state 3 tag three"));
        state3orig.getPartETags().add(new PartETag(4, "state 3 tag four"));
        processor.persistState(cacheKey3, state3orig);
        state3orig.getPartETags().remove(state3orig.getPartETags().size() - 1);
        state3orig.getPartETags().remove(state3orig.getPartETags().size() - 1);
        processor.persistState(cacheKey3, state3orig);

        /*
         * load state and validate that
         *     1. v2 restore shows 4 tags
         *     2. v3 restore shows 2 tags
         */
        final PutS3Object.MultipartState state2new = processor.getState(cacheKey2);
        assertEquals("1234", state2new.getUploadId());
        assertEquals(4, state2new.getPartETags().size());

        final PutS3Object.MultipartState state3new = processor.getState(cacheKey3);
        assertEquals("5678", state3new.getUploadId());
        assertEquals(2, state3new.getPartETags().size());
    }

    @Test
    public void testStateRemove() throws IOException {
        final String bucket = runner.getProcessContext().getProperty(PutS3Object.BUCKET).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3Object.KEY).getValue();
        final String cacheKey = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-sr";

        /*
         * store state, retrieve and validate, remove and validate
         */
        PutS3Object.MultipartState stateOrig = new PutS3Object.MultipartState();
        stateOrig.setUploadId("1234");
        stateOrig.setContentLength(1234L);
        processor.persistState(cacheKey, stateOrig);

        PutS3Object.MultipartState state1 = processor.getState(cacheKey);
        assertEquals("1234", state1.getUploadId());
        assertEquals(1234L, state1.getContentLength().longValue());

        processor.persistState(cacheKey, null);
        PutS3Object.MultipartState state2 = processor.getState(cacheKey);
        assertNull(state2);
    }

    @Test
    public void testApiInteractions() throws IOException {
        final String FILE1_NAME = "file1";
        final String ALPHA_LF = "abcdefghijklmnopqrstuvwxyz\n";
        final String ALPHA_6x = ALPHA_LF + ALPHA_LF + ALPHA_LF + ALPHA_LF + ALPHA_LF + ALPHA_LF;
        final String FILE1_CONTENTS = ALPHA_6x + ALPHA_6x + ALPHA_6x + ALPHA_6x + ALPHA_6x + ALPHA_6x;

        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), FILE1_NAME);
        runner.enqueue(FILE1_CONTENTS.getBytes(), attributes);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, successFiles.size());
        final List<MockFlowFile> failureFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_FAILURE);
        assertEquals(0, failureFiles.size());
        MockFlowFile ff1 = successFiles.get(0);
        assertEquals(FILE1_NAME, ff1.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(BUCKET_NAME, ff1.getAttribute(PutS3Object.S3_BUCKET_KEY));
        assertEquals(SAMPLE_FILE_RESOURCE_NAME, ff1.getAttribute(PutS3Object.S3_OBJECT_KEY));
        assertTrue(reS3ETag.matches(ff1.getAttribute(PutS3Object.S3_ETAG_ATTR_KEY)));
    }

    @Test
    public void testDynamicProperty() throws IOException {
        final String DYNAMIC_ATTRIB_KEY = "fs.runTimestamp";
        final String DYNAMIC_ATTRIB_VALUE = "${now():toNumber()}";

        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);
        PropertyDescriptor testAttrib = processor.getSupportedDynamicPropertyDescriptor(DYNAMIC_ATTRIB_KEY);
        runner.setProperty(testAttrib, DYNAMIC_ATTRIB_VALUE);

        final String FILE1_NAME = "file1";
        Map<String, String> attribs = new HashMap<>();
        attribs.put(CoreAttributes.FILENAME.key(), FILE1_NAME);
        runner.enqueue("123".getBytes(), attribs);

        runner.assertValid();
        processor.getPropertyDescriptor(DYNAMIC_ATTRIB_KEY);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, successFiles.size());
        MockFlowFile ff1 = successFiles.get(0);

        Long now = System.currentTimeMillis();
        String millisNow = Long.toString(now);
        String millisOneSecAgo = Long.toString(now - 1000L);
        String usermeta = ff1.getAttribute(PutS3Object.S3_USERMETA_ATTR_KEY);
        String[] usermetaLine0 = usermeta.split(LINESEP)[0].split("=");
        String usermetaKey0 = usermetaLine0[0];
        String usermetaValue0 = usermetaLine0[1];
        assertEquals(DYNAMIC_ATTRIB_KEY, usermetaKey0);
        assertTrue(usermetaValue0.compareTo(millisOneSecAgo) >=0 && usermetaValue0.compareTo(millisNow) <= 0);
    }

    @Test
    public void testProvenance() throws InitializationException {
        final String PROV1_FILE = "provfile1";
        runner.setProperty(PutS3Object.KEY, "${filename}");

        Map<String, String> attribs = new HashMap<>();
        attribs.put(CoreAttributes.FILENAME.key(), PROV1_FILE);
        runner.enqueue("prov1 contents".getBytes(), attribs);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        assertEquals(1, successFiles.size());

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        ProvenanceEventRecord provRec1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, provRec1.getEventType());
        assertEquals(processor.getIdentifier(), provRec1.getComponentId());
        client.setRegion(Region.fromValue(REGION).toAWSRegion());
        String targetUri = client.getUrl(BUCKET_NAME, PROV1_FILE).toString();
        assertEquals(targetUri, provRec1.getTransitUri());
        assertEquals(6, provRec1.getUpdatedAttributes().size());
        assertEquals(BUCKET_NAME, provRec1.getUpdatedAttributes().get(PutS3Object.S3_BUCKET_KEY));
    }

    public class TestablePutS3Object extends PutS3Object {
        public final String MOCK_CLIENT_PART_ETAG_PREFIX = UUID.nameUUIDFromBytes("PARTETAG".getBytes()).toString();
        public final String MOCK_CLIENT_FILE_ETAG_PREFIX = UUID.nameUUIDFromBytes("FILEETAG".getBytes()).toString();
        public final String MOCK_CLIENT_VERSION = "mock-version";

        @Override
        protected AmazonS3Client createClient(ProcessContext context, AWSCredentials credentials,
                                              ClientConfiguration config) {
            return new MockAWSClient(MOCK_CLIENT_PART_ETAG_PREFIX, MOCK_CLIENT_FILE_ETAG_PREFIX, MOCK_CLIENT_VERSION);
        }
    }

    private class MockAWSClient extends AmazonS3Client {
        private String partETag;
        private String fileETag;
        private String version;

        public MockAWSClient(String partEtag, String fileEtag, String version) {
            this.partETag = partEtag;
            this.fileETag = fileEtag;
            this.version = version;
        }

        @Override
        public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest initiateMultipartUploadRequest) throws AmazonClientException {
            InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
            result.setBucketName(initiateMultipartUploadRequest.getBucketName());
            result.setKey(initiateMultipartUploadRequest.getKey());
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