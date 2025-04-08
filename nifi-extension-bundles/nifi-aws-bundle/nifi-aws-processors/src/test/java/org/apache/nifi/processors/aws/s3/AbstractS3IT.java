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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.CreateKeyRequest;
import com.amazonaws.services.kms.model.CreateKeyResult;
import com.amazonaws.services.kms.model.GenerateDataKeyRequest;
import com.amazonaws.services.kms.model.GenerateDataKeyResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.processors.aws.util.RegionUtilV1;
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
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

    protected final static String SAMPLE_FILE_RESOURCE_NAME = "/hello.txt";
    protected final static String BUCKET_NAME = "test-bucket-" + System.currentTimeMillis();

    private static AmazonS3 client;
    private static AWSKMS kmsClient;
    private final List<String> addedKeys = new ArrayList<>();

    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:latest");

    private static final LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.KMS);

    @BeforeAll
    public static void oneTimeSetup() {
        localstack.start();

        client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(localstack.getEndpoint().toString(), localstack.getRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        kmsClient = AWSKMSClient.builder()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(localstack.getEndpoint().toString(), localstack.getRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        final CreateBucketRequest request = new CreateBucketRequest(BUCKET_NAME);
        client.createBucket(request);
        client.deleteBucketEncryption(BUCKET_NAME);
    }

    @BeforeEach
    public void clearKeys() {
        addedKeys.clear();
    }

    @AfterEach
    public void emptyBucket() {
        if (!client.doesBucketExistV2(BUCKET_NAME)) {
            return;
        }

        ObjectListing objectListing = client.listObjects(BUCKET_NAME);
        while (true) {
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                client.deleteObject(BUCKET_NAME, objectSummary.getKey());
            }

            if (objectListing.isTruncated()) {
                objectListing = client.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
    }

    @AfterAll
    public static void oneTimeTearDown() {
        try {
            if (client == null || !client.doesBucketExistV2(BUCKET_NAME)) {
                return;
            }

            DeleteBucketRequest dbr = new DeleteBucketRequest(BUCKET_NAME);
            client.deleteBucket(dbr);
        } catch (final AmazonS3Exception e) {
            logger.error("Unable to delete bucket {}", BUCKET_NAME, e);
        }
    }

    protected AmazonS3 getClient() {
        return client;
    }

    protected AWSKMS getKmsClient() {
        return kmsClient;
    }

    protected String getEndpointOverride() {
        return localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString();
    }

    protected static String getRegion() {
        return localstack.getRegion();
    }

    protected static void setSecureProperties(final TestRunner runner) throws InitializationException {
        AuthUtils.enableAccessKey(runner, localstack.getAccessKey(), localstack.getSecretKey());
    }

    protected void putTestFile(String key, File file) throws AmazonS3Exception {
        PutObjectRequest putRequest = new PutObjectRequest(BUCKET_NAME, key, file);
        client.putObject(putRequest);
    }

    protected void putTestFileEncrypted(String key, File file) throws AmazonS3Exception, FileNotFoundException {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        PutObjectRequest putRequest = new PutObjectRequest(BUCKET_NAME, key, new FileInputStream(file), objectMetadata);

        client.putObject(putRequest);
    }

    protected void putFileWithUserMetadata(String key, File file, Map<String, String> userMetadata) throws AmazonS3Exception, FileNotFoundException {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setUserMetadata(userMetadata);
        PutObjectRequest putRequest = new PutObjectRequest(BUCKET_NAME, key, new FileInputStream(file), objectMetadata);

        client.putObject(putRequest);
    }

    protected void waitForFilesAvailable() {
        for (final String key : addedKeys) {
            final long maxWaitTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
            while (System.currentTimeMillis() < maxWaitTimestamp) {
                try {
                    client.getObject(BUCKET_NAME, key);
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
        PutObjectRequest putRequest = new PutObjectRequest(BUCKET_NAME, key, file);
        putRequest.setTagging(new ObjectTagging(objectTags));
        client.putObject(putRequest);
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
        CreateKeyRequest cmkRequest = new CreateKeyRequest().withDescription("CMK for unit tests");
        CreateKeyResult cmkResult = kmsClient.createKey(cmkRequest);

        GenerateDataKeyRequest dekRequest = new GenerateDataKeyRequest().withKeyId(cmkResult.getKeyMetadata().getKeyId()).withKeySpec("AES_128");
        GenerateDataKeyResult dekResult = kmsClient.generateDataKey(dekRequest);

        return dekResult.getKeyId();
    }


    protected TestRunner initRunner(final Class<? extends AbstractS3Processor> processorClass) {
        TestRunner runner = TestRunners.newTestRunner(processorClass);

        try {
            setSecureProperties(runner);
        } catch (InitializationException e) {
            Assertions.fail("Could not set security properties");
        }

        runner.setProperty(RegionUtilV1.S3_REGION, getRegion());
        runner.setProperty(AbstractS3Processor.ENDPOINT_OVERRIDE, getEndpointOverride());
        runner.setProperty(AbstractS3Processor.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);

        return runner;
    }

}