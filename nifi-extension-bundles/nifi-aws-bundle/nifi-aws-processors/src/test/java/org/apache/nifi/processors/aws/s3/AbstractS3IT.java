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

import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.processors.aws.util.LocalStackContainers;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Base class for S3 Integration Tests. Establishes a bucket and helper methods for creating test scenarios
 *
 * @see ITDeleteS3Object
 * @see ITFetchS3Object
 * @see ITPutS3Object
 * @see ITListS3
 */
public abstract class AbstractS3IT {
    private static final Logger logger = LoggerFactory.getLogger(AbstractS3IT.class);

    protected static final String SAMPLE_FILE_RESOURCE_NAME = "/hello.txt";
    protected static final String BUCKET_NAME = "test-bucket-" + System.currentTimeMillis();

    private static S3Client client;
    private static KmsClient kmsClient;
    private final List<String> addedKeys = new ArrayList<>();

    private static final LocalStackContainer localstack = LocalStackContainers.newContainer().withServices("s3", "kms");

    @BeforeAll
    public static void oneTimeSetup() {
        localstack.start();

        client = S3Client.builder()
                .region(Region.of(localstack.getRegion()))
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        kmsClient = KmsClient.builder()
                .region(Region.of(localstack.getRegion()))
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        final CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(BUCKET_NAME).build();
        client.createBucket(createBucketRequest);
        final DeleteBucketEncryptionRequest deleteBucketEncryptionRequest = DeleteBucketEncryptionRequest.builder().bucket(BUCKET_NAME).build();
        client.deleteBucketEncryption(deleteBucketEncryptionRequest);
    }

    @BeforeEach
    public void clearKeys() {
        addedKeys.clear();
    }

    @AfterEach
    public void emptyBucket() {
        try {
            client.headBucket(HeadBucketRequest.builder().bucket(BUCKET_NAME).build());
        } catch (NoSuchBucketException nsbe) {
            return;
        }

        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .build();

        ListObjectsV2Iterable list = client.listObjectsV2Paginator(listRequest);

        for (S3Object s3Object : list.contents()) {
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(s3Object.key())
                    .build();
            client.deleteObject(deleteRequest);
        }
    }

    @AfterAll
    public static void oneTimeTearDown() {
        try {
            if (client == null) {
                return;
            }

            try {
                client.headBucket(HeadBucketRequest.builder().bucket(BUCKET_NAME).build());
            } catch (NoSuchBucketException nsbe) {
                return;
            }

            DeleteBucketRequest dbr = DeleteBucketRequest.builder().bucket(BUCKET_NAME).build();
            client.deleteBucket(dbr);
        } catch (final S3Exception e) {
            logger.error("Unable to delete bucket {}", BUCKET_NAME, e);
        }
    }

    protected URI getEndpoint() {
        return localstack.getEndpoint();
    }

    protected S3Client getClient() {
        return client;
    }

    protected String getEndpointOverride() {
        return localstack.getEndpoint().toString();
    }

    protected static String getRegion() {
        return localstack.getRegion();
    }

    protected static void setSecureProperties(final TestRunner runner) throws InitializationException {
        AuthUtils.enableAccessKey(runner, localstack.getAccessKey(), localstack.getSecretKey());
    }

    protected void putTestFile(String key, File file) throws S3Exception {
        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(key)
                .build();

        RequestBody requestBody = RequestBody.fromFile(file);

        client.putObject(putRequest, requestBody);
    }

    protected void putTestFileEncrypted(String key, File file) throws S3Exception {
        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(key)
                .serverSideEncryption(ServerSideEncryption.AES256)
                .build();

        RequestBody requestBody = RequestBody.fromFile(file);

        client.putObject(putRequest, requestBody);
    }

    protected void putFileWithUserMetadata(String key, File file, Map<String, String> userMetadata) throws S3Exception {
        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(key)
                .metadata(userMetadata)
                .build();

        RequestBody requestBody = RequestBody.fromFile(file);

        client.putObject(putRequest, requestBody);
    }

    protected void waitForFilesAvailable() {
        for (final String key : addedKeys) {
            final long maxWaitTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
            final GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(key)
                    .build();
            while (System.currentTimeMillis() < maxWaitTimestamp) {
                try {
                    client.getObject(getRequest);
                } catch (final Exception e) {
                    try {
                        Thread.sleep(100L);
                    } catch (final InterruptedException ie) {
                        throw new AssertionError("Interrupted while waiting for files to become available", e);
                    }
                }
            }
        }
    }

    protected void putFileWithObjectTag(String key, File file, List<Tag> objectTags) {
        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(key)
                .tagging(Tagging.builder()
                        .tagSet(objectTags)
                        .build())
                .build();

        RequestBody requestBody = RequestBody.fromFile(file);

        client.putObject(putRequest, requestBody);
    }

    protected Path getResourcePath(String resourceName) {
        Path path = null;

        try {
            path = Paths.get(getClass().getResource(resourceName).toURI());
        } catch (URISyntaxException e) {
            fail("Resource: " + resourceName + " does not exist" + e.getLocalizedMessage());
        }

        return path;
    }

    protected File getFileFromResourceName(String resourceName) {
        URI uri = null;
        try {
            uri = this.getClass().getResource(resourceName).toURI();
        } catch (URISyntaxException e) {
            fail("Cannot proceed without File : " + resourceName);
        }

        return new File(uri);
    }

    protected static String getKMSKey() {
        CreateKeyRequest cmkRequest = CreateKeyRequest.builder().description("CMK for unit tests").build();
        CreateKeyResponse cmkResponse = kmsClient.createKey(cmkRequest);

        GenerateDataKeyRequest dekRequest = GenerateDataKeyRequest.builder().keyId(cmkResponse.keyMetadata().keyId()).keySpec("AES_128").build();
        GenerateDataKeyResponse dekResponse = kmsClient.generateDataKey(dekRequest);

        return dekResponse.keyId();
    }


    protected TestRunner initRunner(final Class<? extends AbstractS3Processor> processorClass) {
        TestRunner runner = TestRunners.newTestRunner(processorClass);

        try {
            setSecureProperties(runner);
        } catch (InitializationException e) {
            Assertions.fail("Could not set security properties");
        }

        runner.setProperty(RegionUtil.REGION, getRegion());
        runner.setProperty(AbstractS3Processor.ENDPOINT_OVERRIDE, getEndpointOverride());
        runner.setProperty(AbstractS3Processor.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);

        return runner;
    }

}
