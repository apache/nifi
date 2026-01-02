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

import org.apache.commons.lang3.Strings;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.processors.transfer.ResourceTransferSource;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;

import java.io.File;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.nifi.processors.transfer.ResourceTransferProperties.FILE_RESOURCE_SERVICE;
import static org.apache.nifi.processors.transfer.ResourceTransferProperties.RESOURCE_TRANSFER_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPutS3Object {

    private TestRunner runner;
    private PutS3Object putS3Object;
    private S3Client mockS3Client;

    @BeforeEach
    public void setUp() {
        mockS3Client = mock(S3Client.class);
        Mockito.when(mockS3Client.utilities()).thenReturn(S3Utilities.builder().region(Region.US_WEST_2).build());
        putS3Object = new PutS3Object() {
            @Override
            protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(putS3Object);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");

        // MockPropertyValue does not evaluate system properties, set it in a variable with the same name
        runner.setEnvironmentVariableValue("java.io.tmpdir", System.getProperty("java.io.tmpdir"));
    }

    @Test
    public void testPutSinglePartFromLocalFileSource() throws Exception {
        prepareTest();

        String serviceId = "fileresource";
        FileResourceService service = mock(FileResourceService.class);
        InputStream localFileInputStream = mock(InputStream.class);
        when(service.getIdentifier()).thenReturn(serviceId);
        long contentLength = 10L;
        when(service.getFileResource(anyMap())).thenReturn(new FileResource(localFileInputStream, contentLength));

        runner.addControllerService(serviceId, service);
        runner.enableControllerService(service);
        runner.setProperty(RESOURCE_TRANSFER_SOURCE, ResourceTransferSource.FILE_RESOURCE_SERVICE.getValue());
        runner.setProperty(FILE_RESOURCE_SERVICE, serviceId);

        runner.run();

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> captureRequestBody = ArgumentCaptor.forClass(RequestBody.class);
        verify(mockS3Client).putObject(captureRequest.capture(), captureRequestBody.capture());
        PutObjectRequest putObjectRequest = captureRequest.getValue();
        assertEquals(putObjectRequest.contentLength(), contentLength);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testPutSinglePart() {
        runner.setProperty("x-custom-prop", "hello");
        prepareTest();

        runner.run(1);

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture(), Mockito.any(RequestBody.class));
        PutObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);

        ff0.assertAttributeEquals(CoreAttributes.FILENAME.key(), "testfile.txt");
        ff0.assertContentEquals("Test Content");
        ff0.assertAttributeEquals(PutS3Object.S3_ETAG_ATTR_KEY, "test-etag");
        ff0.assertAttributeEquals(PutS3Object.S3_VERSION_ATTR_KEY, "test-version");
    }

    @Test
    public void testPutSinglePartException() {
        prepareTest();

        when(mockS3Client.putObject(Mockito.any(PutObjectRequest.class), Mockito.any(RequestBody.class))).thenThrow(S3Exception.builder().message("TestFail").build());

        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testObjectTags() {
        runner.setProperty(PutS3Object.OBJECT_TAGS_PREFIX, "tagS3");
        runner.setProperty(PutS3Object.REMOVE_TAG_PREFIX, "false");
        prepareTest();

        runner.run(1);

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture(), Mockito.any(RequestBody.class));
        PutObjectRequest request = captureRequest.getValue();

        String tagsString = request.tagging();

        assertEquals("tagS3PII=true", tagsString);
    }

    @ParameterizedTest
    @EnumSource(StorageClass.class)
    public void testStorageClasses(StorageClass storageClass) {
        runner.setProperty(PutS3Object.STORAGE_CLASS, storageClass.name());
        prepareTest();

        runner.run(1);

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture(), Mockito.any(RequestBody.class));
        PutObjectRequest request = captureRequest.getValue();

        assertEquals(storageClass.toString(), request.storageClassAsString());
    }

    @Test
    public void testFilenameWithNationalCharacters() {
        prepareTest("Iñtërnâtiônàližætiøn.txt");

        runner.run(1);

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture(), Mockito.any(RequestBody.class));
        PutObjectRequest request = captureRequest.getValue();

        assertEquals(URLEncoder.encode("Iñtërnâtiônàližætiøn.txt", UTF_8), request.contentDisposition());
    }

    @Test
    public void testRegionFromFlowFileAttribute() {
        runner.setProperty(PutS3Object.OBJECT_TAGS_PREFIX, "tagS3");
        runner.setProperty(PutS3Object.REMOVE_TAG_PREFIX, "false");
        prepareTestWithRegionInAttributes("testfile.txt", "us-east-1");

        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }

    private void prepareTest() {
        prepareTest("testfile.txt");
    }

    private void prepareTest(String filename) {
        runner.setProperty(RegionUtil.REGION, "ap-northeast-1");
        runner.setProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.assertValid();

        Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", filename);
        ffAttributes.put("tagS3PII", "true");
        runner.enqueue("Test Content", ffAttributes);

        initMocks();
    }

    private void prepareTestWithRegionInAttributes(String filename, String region) {
        runner.setProperty(RegionUtil.REGION, "use-custom-region");
        runner.setProperty(RegionUtil.CUSTOM_REGION, "${s3.region}");
        runner.setProperty(PutS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.assertValid();

        Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("s3.region", region);
        ffAttributes.put("filename", filename);
        ffAttributes.put("tagS3PII", "true");
        runner.enqueue("Test Content", ffAttributes);

        initMocks();
    }

    private void initMocks() {
        PutObjectResponse putObjectResult = PutObjectResponse.builder()
                .eTag("test-etag")
                .versionId("test-version")
                .build();

        when(mockS3Client.putObject(Mockito.any(PutObjectRequest.class), Mockito.any(RequestBody.class))).thenReturn(putObjectResult);

        ListMultipartUploadsResponse uploadListing = ListMultipartUploadsResponse.builder().build();
        when(mockS3Client.listMultipartUploads(Mockito.any(ListMultipartUploadsRequest.class))).thenReturn(uploadListing);
    }

    @Test
    public void testPersistenceFileLocationWithDefaultTempDir() {
        String dir = System.getProperty("java.io.tmpdir");

        executePersistenceFileLocationTest(Strings.CS.appendIfMissing(dir, File.separator) + putS3Object.getIdentifier());
    }

    @Test
    public void testPersistenceFileLocationWithUserDefinedDirWithEndingSeparator() {
        String dir = Strings.CS.appendIfMissing(new File("target").getAbsolutePath(), File.separator);
        runner.setProperty(PutS3Object.MULTIPART_TEMP_DIR, dir);

        executePersistenceFileLocationTest(dir + putS3Object.getIdentifier());
    }

    @Test
    public void testPersistenceFileLocationWithUserDefinedDirWithoutEndingSeparator() {
        String dir = Strings.CS.removeEnd(new File("target").getAbsolutePath(), File.separator);
        runner.setProperty(PutS3Object.MULTIPART_TEMP_DIR, dir);

        executePersistenceFileLocationTest(dir + File.separator + putS3Object.getIdentifier());
    }

    private void executePersistenceFileLocationTest(String expectedPath) {
        prepareTest();

        runner.run(1);
        File file = putS3Object.getPersistenceFile();

        assertEquals(expectedPath, file.getAbsolutePath());
    }

    @Test
    void testMigration() {
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expectedRenamed =
                Map.of("canned-acl", AbstractS3Processor.CANNED_ACL.getName(),
                        "encryption-service", AbstractS3Processor.ENCRYPTION_SERVICE.getName(),
                        "use-chunked-encoding", AbstractS3Processor.USE_CHUNKED_ENCODING.getName(),
                        "use-path-style-access", AbstractS3Processor.USE_PATH_STYLE_ACCESS.getName(),
                        "s3-object-tags-prefix", PutS3Object.OBJECT_TAGS_PREFIX.getName(),
                        "s3-object-remove-tags-prefix", PutS3Object.REMOVE_TAG_PREFIX.getName(),
                        "s3-temporary-directory-multipart", PutS3Object.MULTIPART_TEMP_DIR.getName());

        expectedRenamed.forEach((key, value) -> assertEquals(value, propertyMigrationResult.getPropertiesRenamed().get(key)));
    }
}
