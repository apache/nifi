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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.StorageClass;

/**
 * Provides integration level testing with actual AWS S3 resources for {@link PutS3Object} and requires additional configuration and resources to work.
 */
public class ITPutS3Object extends AbstractS3IT {

    final static String TEST_ENDPOINT = "https://endpoint.com";
    //    final static String TEST_TRANSIT_URI = "https://" + BUCKET_NAME + ".endpoint.com";
    final static String TEST_PARTSIZE_STRING = "50 mb";
    final static Long   TEST_PARTSIZE_LONG = 50L * 1024L * 1024L;

    final static Long S3_MINIMUM_PART_SIZE = 50L * 1024L * 1024L;
    final static Long S3_MAXIMUM_OBJECT_SIZE = 5L * 1024L * 1024L * 1024L;

    final static Pattern reS3ETag = Pattern.compile("[0-9a-fA-f]{32,32}(-[0-9]+)?");

    @Test
    public void testSimplePut() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);

        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        for (int i = 0; i < 3; i++) {
            final Map<String, String> attrs = new HashMap<>();
            attrs.put("filename", String.valueOf(i) + ".txt");
            runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        }
        runner.run(3);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 3);
    }

    @Test
    public void testSimplePutEncrypted() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.SERVER_SIDE_ENCRYPTION, ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        for (int i = 0; i < 3; i++) {
            final Map<String, String> attrs = new HashMap<>();
            attrs.put("filename", String.valueOf(i) + ".txt");
            runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        }
        runner.run(3);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 3);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        for (MockFlowFile flowFile : ffs) {
            flowFile.assertAttributeEquals(PutS3Object.S3_SSE_ALGORITHM, ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }
    }

    private void testPutThenFetch(String sseAlgorithm) throws IOException {

        // Put
        TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        if(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION.equals(sseAlgorithm)){
            runner.setProperty(PutS3Object.SERVER_SIDE_ENCRYPTION, sseAlgorithm);
        }

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename",  "filename-on-s3.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
        List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        if(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION.equals(sseAlgorithm)){
            ffs.get(0).assertAttributeEquals(PutS3Object.S3_SSE_ALGORITHM, ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        } else {
            ffs.get(0).assertAttributeNotExists(PutS3Object.S3_SSE_ALGORITHM);
        }

        // Fetch
        runner = TestRunners.newTestRunner(new FetchS3Object());

        runner.setProperty(FetchS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(FetchS3Object.REGION, REGION);
        runner.setProperty(FetchS3Object.BUCKET, BUCKET_NAME);

        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);
        ff.assertContentEquals(getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        if(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION.equals(sseAlgorithm)){
            ff.assertAttributeEquals(PutS3Object.S3_SSE_ALGORITHM, ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        } else {
            ff.assertAttributeNotExists(PutS3Object.S3_SSE_ALGORITHM);
        }

    }


    @Test
    public void testPutThenFetchWithoutSSE() throws IOException {
        testPutThenFetch(PutS3Object.NO_SERVER_SIDE_ENCRYPTION);
    }

    @Test
    public void testPutThenFetchWithSSE() throws IOException {
        testPutThenFetch(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    }


    @Test
    public void testPutS3ObjectUsingCredentialsProviderService() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AbstractAWSCredentialsProviderProcessor.CREDENTIALS_FILE, System.getProperty("user.home") + "/aws-credentials.properties");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        runner.setProperty(PutS3Object.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);

        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        for (int i = 0; i < 3; i++) {
            final Map<String, String> attrs = new HashMap<>();
            attrs.put("filename", String.valueOf(i) + ".txt");
            runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        }
        runner.run(3);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 3);

    }

    @Test
    public void testMetaData() throws IOException {
        PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        PropertyDescriptor prop1 = processor.getSupportedDynamicPropertyDescriptor("TEST-PROP-1");
        runner.setProperty(prop1, "TESTING-1-2-3");
        PropertyDescriptor prop2 = processor.getSupportedDynamicPropertyDescriptor("TEST-PROP-2");
        runner.setProperty(prop2, "TESTING-4-5-6");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "meta.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        MockFlowFile ff1 = flowFiles.get(0);
        for (Map.Entry attrib : ff1.getAttributes().entrySet()) {
            System.out.println(attrib.getKey() + " = " + attrib.getValue());
        }
    }

    @Test
    public void testContentType() throws IOException {
        PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.CONTENT_TYPE, "text/plain");

        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertAttributeEquals(PutS3Object.S3_CONTENT_TYPE, "text/plain");
    }

    @Test
    public void testPutInFolder() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());
        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);

        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());
        runner.assertValid();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/1.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testStorageClass() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.STORAGE_CLASS, StorageClass.ReducedRedundancy.name());

        int bytesNeeded = 55 * 1024 * 1024;
        StringBuilder bldr = new StringBuilder(bytesNeeded + 1000);
        for (int line = 0; line < 55; line++) {
            bldr.append(String.format("line %06d This is sixty-three characters plus the EOL marker!\n", line));
        }
        String data55mb = bldr.toString();

        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/2.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        attrs.put("filename", "folder/3.txt");
        runner.enqueue(data55mb.getBytes(), attrs);

        runner.run(2);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 2);
        FlowFile file1 = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS).get(0);
        Assert.assertEquals(StorageClass.ReducedRedundancy.toString(),
                file1.getAttribute(PutS3Object.S3_STORAGECLASS_ATTR_KEY));
        FlowFile file2 = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS).get(1);
        Assert.assertEquals(StorageClass.ReducedRedundancy.toString(),
                file2.getAttribute(PutS3Object.S3_STORAGECLASS_ATTR_KEY));
    }

    @Test
    public void testPermissions() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.FULL_CONTROL_USER_LIST,"28545acd76c35c7e91f8409b95fd1aa0c0914bfa1ac60975d9f48bc3c5e090b5");
        runner.setProperty(PutS3Object.REGION, REGION);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/4.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testDynamicProperty() throws IOException {
        final String DYNAMIC_ATTRIB_KEY = "fs.runTimestamp";
        final String DYNAMIC_ATTRIB_VALUE = "${now():toNumber()}";

        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
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
        Assert.assertEquals(1, successFiles.size());
        MockFlowFile ff1 = successFiles.get(0);

        Long now = System.currentTimeMillis();
        String millisNow = Long.toString(now);
        String millisOneSecAgo = Long.toString(now - 1000L);
        String usermeta = ff1.getAttribute(PutS3Object.S3_USERMETA_ATTR_KEY);
        String[] usermetaLine0 = usermeta.split(System.lineSeparator())[0].split("=");
        String usermetaKey0 = usermetaLine0[0];
        String usermetaValue0 = usermetaLine0[1];
        Assert.assertEquals(DYNAMIC_ATTRIB_KEY, usermetaKey0);
        Assert.assertTrue(usermetaValue0.compareTo(millisOneSecAgo) >=0 && usermetaValue0.compareTo(millisNow) <= 0);
    }

    @Test
    public void testProvenance() throws InitializationException {
        final String PROV1_FILE = "provfile1";

        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.KEY, "${filename}");

        Map<String, String> attribs = new HashMap<>();
        attribs.put(CoreAttributes.FILENAME.key(), PROV1_FILE);
        runner.enqueue("prov1 contents".getBytes(), attribs);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        Assert.assertEquals(1, successFiles.size());

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        Assert.assertEquals(1, provenanceEvents.size());
        ProvenanceEventRecord provRec1 = provenanceEvents.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, provRec1.getEventType());
        Assert.assertEquals(processor.getIdentifier(), provRec1.getComponentId());
        client.setRegion(Region.fromValue(REGION).toAWSRegion());
        String targetUri = client.getUrl(BUCKET_NAME, PROV1_FILE).toString();
        Assert.assertEquals(targetUri, provRec1.getTransitUri());
        Assert.assertEquals(7, provRec1.getUpdatedAttributes().size());
        Assert.assertEquals(BUCKET_NAME, provRec1.getUpdatedAttributes().get(PutS3Object.S3_BUCKET_KEY));
    }

    @Test
    public void testStateDefaults() {
        PutS3Object.MultipartState state1 = new PutS3Object.MultipartState();
        Assert.assertEquals(state1.getUploadId(), "");
        Assert.assertEquals(state1.getFilePosition(), (Long) 0L);
        Assert.assertEquals(state1.getPartETags().size(), 0L);
        Assert.assertEquals(state1.getPartSize(), (Long) 0L);
        Assert.assertEquals(state1.getStorageClass().toString(), StorageClass.Standard.toString());
        Assert.assertEquals(state1.getContentLength(), (Long) 0L);
    }

    @Test
    public void testStateToString() throws IOException, InitializationException {
        final String target = "UID-test1234567890#10001#1/PartETag-1,2/PartETag-2,3/PartETag-3,4/PartETag-4#20002#REDUCED_REDUNDANCY#30003#8675309";
        PutS3Object.MultipartState state2 = new PutS3Object.MultipartState();
        state2.setUploadId("UID-test1234567890");
        state2.setFilePosition(10001L);
        state2.setTimestamp(8675309L);
        for (Integer partNum = 1; partNum < 5; partNum++) {
            state2.addPartETag(new PartETag(partNum, "PartETag-" + partNum.toString()));
        }
        state2.setPartSize(20002L);
        state2.setStorageClass(StorageClass.ReducedRedundancy);
        state2.setContentLength(30003L);
        Assert.assertEquals(target, state2.toString());
    }

    @Test
    public void testEndpointOverride() {
        // remove leading "/" from filename to avoid duplicate separators
        final String TESTKEY = AbstractS3IT.SAMPLE_FILE_RESOURCE_NAME.substring(1);

        final PutS3Object processor = new TestablePutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);
        final ProcessContext context = runner.getProcessContext();

        runner.setProperty(PutS3Object.ENDPOINT_OVERRIDE, TEST_ENDPOINT);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.KEY, TESTKEY);

        runner.run();

        Assert.assertEquals(BUCKET_NAME, context.getProperty(PutS3Object.BUCKET).toString());
        Assert.assertEquals(TESTKEY, context.getProperty(PutS3Object.KEY).evaluateAttributeExpressions().toString());
        Assert.assertEquals(TEST_ENDPOINT, context.getProperty(PutS3Object.ENDPOINT_OVERRIDE).toString());

        String s3url = ((TestablePutS3Object)processor).testable_getClient().getResourceUrl(BUCKET_NAME, TESTKEY);
        Assert.assertEquals(TEST_ENDPOINT + "/" + BUCKET_NAME + "/" + TESTKEY, s3url);
    }

    @Test
    public void testMultipartProperties() throws IOException {
        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);
        final ProcessContext context = runner.getProcessContext();

        runner.setProperty(PutS3Object.FULL_CONTROL_USER_LIST,
                "28545acd76c35c7e91f8409b95fd1aa0c0914bfa1ac60975d9f48bc3c5e090b5");
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.KEY, AbstractS3IT.SAMPLE_FILE_RESOURCE_NAME);

        Assert.assertEquals(BUCKET_NAME, context.getProperty(PutS3Object.BUCKET).toString());
        Assert.assertEquals(SAMPLE_FILE_RESOURCE_NAME, context.getProperty(PutS3Object.KEY).evaluateAttributeExpressions().toString());
        Assert.assertEquals(TEST_PARTSIZE_LONG.longValue(),
                context.getProperty(PutS3Object.MULTIPART_PART_SIZE).asDataSize(DataUnit.B).longValue());
    }

    @Test
    public void testLocalStatePersistence() throws IOException {
        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final String bucket = runner.getProcessContext().getProperty(PutS3Object.BUCKET).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3Object.KEY).getValue();
        final String cacheKey1 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key;
        final String cacheKey2 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-v2";
        final String cacheKey3 = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-v3";

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

        final List<MultipartUpload> uploadList = new ArrayList<>();
        final MultipartUpload upload1 = new MultipartUpload();
        upload1.setKey(key);
        upload1.setUploadId("");
        uploadList.add(upload1);
        final MultipartUpload upload2 = new MultipartUpload();
        upload2.setKey(key + "-v2");
        upload2.setUploadId("1234");
        uploadList.add(upload2);
        final MultipartUpload upload3 = new MultipartUpload();
        upload3.setKey(key + "-v3");
        upload3.setUploadId("5678");
        uploadList.add(upload3);
        final MultipartUploadListing uploadListing = new MultipartUploadListing();
        uploadListing.setMultipartUploads(uploadList);
        final MockAmazonS3Client mockClient = new MockAmazonS3Client();
        mockClient.setListing(uploadListing);

        /*
         * reload and validate stored state
         */
        final PutS3Object.MultipartState state1new = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey1);
        Assert.assertEquals("", state1new.getUploadId());
        Assert.assertEquals(0L, state1new.getFilePosition().longValue());
        Assert.assertEquals(new ArrayList<PartETag>(), state1new.getPartETags());
        Assert.assertEquals(0L, state1new.getPartSize().longValue());
        Assert.assertEquals(StorageClass.fromValue(StorageClass.Standard.toString()), state1new.getStorageClass());
        Assert.assertEquals(0L, state1new.getContentLength().longValue());

        final PutS3Object.MultipartState state2new = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey2);
        Assert.assertEquals("1234", state2new.getUploadId());
        Assert.assertEquals(0L, state2new.getFilePosition().longValue());
        Assert.assertEquals(new ArrayList<PartETag>(), state2new.getPartETags());
        Assert.assertEquals(0L, state2new.getPartSize().longValue());
        Assert.assertEquals(StorageClass.fromValue(StorageClass.Standard.toString()), state2new.getStorageClass());
        Assert.assertEquals(1234L, state2new.getContentLength().longValue());

        final PutS3Object.MultipartState state3new = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey3);
        Assert.assertEquals("5678", state3new.getUploadId());
        Assert.assertEquals(0L, state3new.getFilePosition().longValue());
        Assert.assertEquals(new ArrayList<PartETag>(), state3new.getPartETags());
        Assert.assertEquals(0L, state3new.getPartSize().longValue());
        Assert.assertEquals(StorageClass.fromValue(StorageClass.Standard.toString()), state3new.getStorageClass());
        Assert.assertEquals(5678L, state3new.getContentLength().longValue());
    }

    @Test
    public void testStatePersistsETags() throws IOException {
        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final String bucket = runner.getProcessContext().getProperty(PutS3Object.BUCKET).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3Object.KEY).getValue();
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
        state2orig.getPartETags().add(new PartETag(1, "state 2 tag one"));
        state2orig.getPartETags().add(new PartETag(2, "state 2 tag two"));
        processor.persistLocalState(cacheKey2, state2orig);
        state2orig.getPartETags().add(new PartETag(3, "state 2 tag three"));
        state2orig.getPartETags().add(new PartETag(4, "state 2 tag four"));
        processor.persistLocalState(cacheKey2, state2orig);

        state3orig.getPartETags().add(new PartETag(1, "state 3 tag one"));
        state3orig.getPartETags().add(new PartETag(2, "state 3 tag two"));
        state3orig.getPartETags().add(new PartETag(3, "state 3 tag three"));
        state3orig.getPartETags().add(new PartETag(4, "state 3 tag four"));
        processor.persistLocalState(cacheKey3, state3orig);
        state3orig.getPartETags().remove(state3orig.getPartETags().size() - 1);
        state3orig.getPartETags().remove(state3orig.getPartETags().size() - 1);
        processor.persistLocalState(cacheKey3, state3orig);

        final List<MultipartUpload> uploadList = new ArrayList<>();
        final MultipartUpload upload1 = new MultipartUpload();
        upload1.setKey(key + "-bv2");
        upload1.setUploadId("1234");
        uploadList.add(upload1);
        final MultipartUpload upload2 = new MultipartUpload();
        upload2.setKey(key + "-bv3");
        upload2.setUploadId("5678");
        uploadList.add(upload2);
        final MultipartUploadListing uploadListing = new MultipartUploadListing();
        uploadListing.setMultipartUploads(uploadList);
        final MockAmazonS3Client mockClient = new MockAmazonS3Client();
        mockClient.setListing(uploadListing);

        /*
         * load state and validate that
         *     1. v2 restore shows 4 tags
         *     2. v3 restore shows 2 tags
         */
        final PutS3Object.MultipartState state2new = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey2);
        Assert.assertEquals("1234", state2new.getUploadId());
        Assert.assertEquals(4, state2new.getPartETags().size());

        final PutS3Object.MultipartState state3new = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey3);
        Assert.assertEquals("5678", state3new.getUploadId());
        Assert.assertEquals(2, state3new.getPartETags().size());
    }

    @Test
    public void testStateRemove() throws IOException {
        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final String bucket = runner.getProcessContext().getProperty(PutS3Object.BUCKET).getValue();
        final String key = runner.getProcessContext().getProperty(PutS3Object.KEY).getValue();
        final String cacheKey = runner.getProcessor().getIdentifier() + "/" + bucket + "/" + key + "-sr";

        final List<MultipartUpload> uploadList = new ArrayList<>();
        final MultipartUpload upload1 = new MultipartUpload();
        upload1.setKey(key);
        upload1.setUploadId("1234");
        uploadList.add(upload1);
        final MultipartUploadListing uploadListing = new MultipartUploadListing();
        uploadListing.setMultipartUploads(uploadList);
        final MockAmazonS3Client mockClient = new MockAmazonS3Client();
        mockClient.setListing(uploadListing);

        /*
         * store state, retrieve and validate, remove and validate
         */
        PutS3Object.MultipartState stateOrig = new PutS3Object.MultipartState();
        stateOrig.setUploadId("1234");
        stateOrig.setContentLength(1234L);
        processor.persistLocalState(cacheKey, stateOrig);

        PutS3Object.MultipartState state1 = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey);
        Assert.assertEquals("1234", state1.getUploadId());
        Assert.assertEquals(1234L, state1.getContentLength().longValue());

        processor.persistLocalState(cacheKey, null);
        PutS3Object.MultipartState state2 = processor.getLocalStateIfInS3(mockClient, bucket, cacheKey);
        Assert.assertNull(state2);
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
        System.out.println("file size: " + tempByteCount);
        Assert.assertTrue(tempByteCount < S3_MINIMUM_PART_SIZE);

        Assert.assertTrue(megabyte.length < S3_MINIMUM_PART_SIZE);
        Assert.assertTrue(TEST_PARTSIZE_LONG >= S3_MINIMUM_PART_SIZE && TEST_PARTSIZE_LONG <= S3_MAXIMUM_OBJECT_SIZE);

        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), FILE1_NAME);
        runner.enqueue(new FileInputStream(tempFile.toFile()), attributes);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        Assert.assertEquals(1, successFiles.size());
        final List<MockFlowFile> failureFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_FAILURE);
        Assert.assertEquals(0, failureFiles.size());
        MockFlowFile ff1 = successFiles.get(0);
        Assert.assertEquals(PutS3Object.S3_API_METHOD_PUTOBJECT, ff1.getAttribute(PutS3Object.S3_API_METHOD_ATTR_KEY));
        Assert.assertEquals(FILE1_NAME, ff1.getAttribute(CoreAttributes.FILENAME.key()));
        Assert.assertEquals(BUCKET_NAME, ff1.getAttribute(PutS3Object.S3_BUCKET_KEY));
        Assert.assertEquals(FILE1_NAME, ff1.getAttribute(PutS3Object.S3_OBJECT_KEY));
        Assert.assertTrue(reS3ETag.matcher(ff1.getAttribute(PutS3Object.S3_ETAG_ATTR_KEY)).matches());
        Assert.assertEquals(tempByteCount, ff1.getSize());
    }

    @Test
    public void testMultipartBetweenMinimumAndMaximum() throws IOException {
        final String FILE1_NAME = "file1";

        final byte[] megabyte = new byte[1024 * 1024];
        final Path tempFile = Files.createTempFile("s3mulitpart", "tmp");
        final FileOutputStream tempOut = new FileOutputStream(tempFile.toFile());
        long tempByteCount = 0;
        for ( ; tempByteCount < TEST_PARTSIZE_LONG + 1; ) {
            tempOut.write(megabyte);
            tempByteCount += megabyte.length;
        }
        tempOut.close();
        System.out.println("file size: " + tempByteCount);
        Assert.assertTrue(tempByteCount > S3_MINIMUM_PART_SIZE && tempByteCount < S3_MAXIMUM_OBJECT_SIZE);
        Assert.assertTrue(tempByteCount > TEST_PARTSIZE_LONG);

        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.MULTIPART_THRESHOLD, TEST_PARTSIZE_STRING);
        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), FILE1_NAME);
        runner.enqueue(new FileInputStream(tempFile.toFile()), attributes);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        Assert.assertEquals(1, successFiles.size());
        final List<MockFlowFile> failureFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_FAILURE);
        Assert.assertEquals(0, failureFiles.size());
        MockFlowFile ff1 = successFiles.get(0);
        Assert.assertEquals(PutS3Object.S3_API_METHOD_MULTIPARTUPLOAD, ff1.getAttribute(PutS3Object.S3_API_METHOD_ATTR_KEY));
        Assert.assertEquals(FILE1_NAME, ff1.getAttribute(CoreAttributes.FILENAME.key()));
        Assert.assertEquals(BUCKET_NAME, ff1.getAttribute(PutS3Object.S3_BUCKET_KEY));
        Assert.assertEquals(FILE1_NAME, ff1.getAttribute(PutS3Object.S3_OBJECT_KEY));
            Assert.assertTrue(reS3ETag.matcher(ff1.getAttribute(PutS3Object.S3_ETAG_ATTR_KEY)).matches());
        Assert.assertEquals(tempByteCount, ff1.getSize());
    }

    @Ignore
    @Test
    public void testMultipartLargerThanObjectMaximum() throws IOException {
        final String FILE1_NAME = "file1";

        final byte[] megabyte = new byte[1024 * 1024];
        final Path tempFile = Files.createTempFile("s3mulitpart", "tmp");
        final FileOutputStream tempOut = new FileOutputStream(tempFile.toFile());
        for (int i = 0; i < (S3_MAXIMUM_OBJECT_SIZE / 1024 / 1024 + 1); i++) {
            tempOut.write(megabyte);
        }
        tempOut.close();

        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.MULTIPART_PART_SIZE, TEST_PARTSIZE_STRING);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), FILE1_NAME);
        runner.enqueue(new FileInputStream(tempFile.toFile()), attributes);

        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        Assert.assertEquals(1, successFiles.size());
        final List<MockFlowFile> failureFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_FAILURE);
        Assert.assertEquals(0, failureFiles.size());
        MockFlowFile ff1 = successFiles.get(0);
        Assert.assertEquals(FILE1_NAME, ff1.getAttribute(CoreAttributes.FILENAME.key()));
        Assert.assertEquals(BUCKET_NAME, ff1.getAttribute(PutS3Object.S3_BUCKET_KEY));
        Assert.assertEquals(FILE1_NAME, ff1.getAttribute(PutS3Object.S3_OBJECT_KEY));
        Assert.assertTrue(reS3ETag.matcher(ff1.getAttribute(PutS3Object.S3_ETAG_ATTR_KEY)).matches());
        Assert.assertTrue(ff1.getSize() > S3_MAXIMUM_OBJECT_SIZE);
    }

    @Ignore
    @Test
    public void testS3MultipartAgeoff() throws InterruptedException, IOException {
        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);
        final ProcessContext context = runner.getProcessContext();

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);

        // set check interval and age off to minimum values
        runner.setProperty(PutS3Object.MULTIPART_S3_AGEOFF_INTERVAL, "1 milli");
        runner.setProperty(PutS3Object.MULTIPART_S3_MAX_AGE, "1 milli");

        // create some dummy uploads
        for (Integer i = 0; i < 3; i++) {
            final InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(
                    BUCKET_NAME, "file" + i.toString() + ".txt");
            try {
                client.initiateMultipartUpload(initiateRequest);
            } catch (AmazonClientException e) {
                Assert.fail("Failed to initiate upload: " + e.getMessage());
            }
        }

        // Age off is time dependent, so test has some timing constraints.  This
        // sleep() delays long enough to satisfy interval and age intervals.
        Thread.sleep(2000L);

        // System millis are used for timing, but it is incremented on each
        // call to circumvent what appears to be caching in the AWS library.
        // The increments are 1000 millis because AWS returns upload
        // initiation times in whole seconds.
        Long now = System.currentTimeMillis();

        MultipartUploadListing uploadList = processor.getS3AgeoffListAndAgeoffLocalState(context, client, now);
        Assert.assertEquals(3, uploadList.getMultipartUploads().size());

        MultipartUpload upload0 = uploadList.getMultipartUploads().get(0);
        processor.abortS3MultipartUpload(client, BUCKET_NAME, upload0);

        uploadList = processor.getS3AgeoffListAndAgeoffLocalState(context, client, now+1000);
        Assert.assertEquals(2, uploadList.getMultipartUploads().size());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-upload.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);
        runner.run();

        uploadList = processor.getS3AgeoffListAndAgeoffLocalState(context, client, now+2000);
        Assert.assertEquals(0, uploadList.getMultipartUploads().size());
    }

    private class MockAmazonS3Client extends AmazonS3Client {
        MultipartUploadListing listing;
        public void setListing(MultipartUploadListing newlisting) {
            listing = newlisting;
        }

        @Override
        public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest listMultipartUploadsRequest)
                throws AmazonClientException, AmazonServiceException {
            return listing;
        }
    }

    public class TestablePutS3Object extends PutS3Object {
        public AmazonS3Client testable_getClient() {
            return this.getClient();
        }
    }
}
