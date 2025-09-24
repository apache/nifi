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
package org.apache.nifi.registry.aws;

import org.apache.nifi.registry.extension.BundleCoordinate;
import org.apache.nifi.registry.extension.BundlePersistenceContext;
import org.apache.nifi.registry.extension.BundlePersistenceProvider;
import org.apache.nifi.registry.extension.BundleVersionCoordinate;
import org.apache.nifi.registry.extension.BundleVersionType;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3BundlePersistenceProviderIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3BundlePersistenceProviderIT.class);

    private S3Client s3Client;
    private String bucketName;
    private BundlePersistenceProvider provider;
    private ProviderConfigurationContext configurationContext;

    @BeforeEach
    public void setup() {
        final Region region = Region.US_EAST_1;
        final String forcePathStyle =
                //"false";                  // When using AWS S3
                "true";                     // When using Minio
        bucketName = "integration-test-" + System.currentTimeMillis();
        final String endpointUrl =
                //null;                     // When using AWS S3
                "http://localhost:9000";    // When using Minio


        // Create config context and provider, and call onConfigured
        final Map<String, String> properties = new HashMap<>();
        properties.put(S3BundlePersistenceProvider.REGION_PROP, region.id());
        properties.put(S3BundlePersistenceProvider.BUCKET_NAME_PROP, bucketName);
        properties.put(S3BundlePersistenceProvider.CREDENTIALS_PROVIDER_PROP,
                S3BundlePersistenceProvider.CredentialProvider.DEFAULT_CHAIN.name());
        properties.put(S3BundlePersistenceProvider.ENDPOINT_URL_PROP, endpointUrl);
        properties.put(S3BundlePersistenceProvider.FORCE_PATH_STYLE_PROP, forcePathStyle);

        configurationContext = mock(ProviderConfigurationContext.class);
        when(configurationContext.getProperties()).thenReturn(properties);

        provider = new S3BundlePersistenceProvider();
        provider.onConfigured(configurationContext);

        // Create a separate client just for the IT test so we can set up a new bucket
        s3Client = ((S3BundlePersistenceProvider) provider).createS3Client(configurationContext);

        final CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(bucketName)
                .build();

        s3Client.createBucket(createBucketRequest);
        LOGGER.info("Created bucket: {}", bucketName);
    }

    @AfterEach
    public void teardown() {
        try {
            provider.preDestruction();
        } catch (Exception e) {
            LOGGER.warn("Error during provider destruction", e);
        }

        try {
            s3Client.close();
        } catch (Exception e) {
            LOGGER.warn("Error closing S3 client", e);
        }
    }

    private void listBucketContents() {
        try {
            ListObjectsResponse response = s3Client.listObjects(ListObjectsRequest.builder()
                    .bucket(bucketName)
                    .build());

            LOGGER.info("Bucket '{}' contents ({} objects):", bucketName, response.contents().size());
            for (S3Object s3Object : response.contents()) {
                LOGGER.info(" - Key: {}, Size: {}, LastModified: {}",
                        s3Object.key(), s3Object.size(), s3Object.lastModified());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to list bucket contents", e);
        }
    }

    private String getKeyViaReflection(BundleVersionCoordinate coordinate) {
        try {
            Method getKeyMethod = S3BundlePersistenceProvider.class.getDeclaredMethod("getKey", BundleVersionCoordinate.class);
            getKeyMethod.setAccessible(true);
            return (String) getKeyMethod.invoke(provider, coordinate);
        } catch (Exception e) {
            LOGGER.error("Failed to get key via reflection", e);
            return null;
        }
    }

    private boolean objectExists(String key) {
        try {
            HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());
            LOGGER.info("Object exists - Key: {}, Size: {}", key, response.contentLength());
            return true;
        } catch (Exception e) {
            LOGGER.warn("Object does not exist - Key: {}", key);
            return false;
        }
    }

    @Test
    @Disabled // Remove to run this against S3, assumes you have set up external credentials
    /*
     * How to set up local MinIO for testing:
     *
     * 1. Install Docker if not already installed
     *
     * 2. Create data directory for MinIO:
     *    mkdir -p ~/minio/data
     *
     * 3. Start MinIO container:
     *    docker run -d \
     *      -p 9000:9000 \
     *      -p 9001:9001 \
     *      -v ~/minio/data:/data \
     *      -e "MINIO_ROOT_USER=minioadmin" \
     *      -e "MINIO_ROOT_PASSWORD=minioadmin" \
     *      --name minio \
     *      minio/minio server /data --console-address ":9001"
     *
     * 4. Install AWS CLI if not already installed:
     *    # For Ubuntu/Debian:
     *    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
     *    unzip awscliv2.zip
     *    sudo ./aws/install
     *
     * 5. Configure AWS CLI for MinIO:
     *    aws configure set aws_access_key_id minioadmin
     *    aws configure set aws_secret_access_key minioadmin
     *    aws configure set default.region us-east-1
     *    aws configure set default.output json
     *    aws configure set default.s3.endpoint_url http://localhost:9000
     *    aws configure set default.s3.signature_version s3v4
     *
     * 6. Verify MinIO is accessible:
     *    aws --endpoint-url http://localhost:9000 s3 ls
     *
     * 7. Access MinIO web console at: http://localhost:9001
     *    Username: minioadmin
     *    Password: minioadmin
     *
     * 8. Remove @Disabled annotation from this test to run it against local MinIO
     */
    public void testS3PersistenceProvider() throws IOException {
        final File narFile = new File("src/test/resources/nars/nifi-foo-nar-1.0.0.nar");
        if (!narFile.exists()) {
            fail("Test NAR file not found: " + narFile.getAbsolutePath());
        }

        final UUID bucketId = UUID.randomUUID();

        // Save bundle version #1
        final BundleVersionCoordinate versionCoordinate1 = mock(BundleVersionCoordinate.class);
        when(versionCoordinate1.getBucketId()).thenReturn(bucketId.toString());
        when(versionCoordinate1.getGroupId()).thenReturn("org.apache.nifi");
        when(versionCoordinate1.getArtifactId()).thenReturn("nifi-foo-nar");
        when(versionCoordinate1.getVersion()).thenReturn("1.0.0");
        when(versionCoordinate1.getType()).thenReturn(BundleVersionType.NIFI_NAR);

        final BundlePersistenceContext context1 = mock(BundlePersistenceContext.class);
        when(context1.getCoordinate()).thenReturn(versionCoordinate1);
        when(context1.getSize()).thenReturn(narFile.length());

        LOGGER.info("Creating bundle version 1.0.0...");
        try (final InputStream in = new FileInputStream(narFile)) {
            provider.createBundleVersion(context1, in);
            LOGGER.info("Bundle version 1.0.0 created successfully");
        }

        // Verify object was created in S3
        String key1 = getKeyViaReflection(versionCoordinate1);
        LOGGER.info("Expected key for version 1.0.0: {}", key1);
        assertTrue(objectExists(key1), "Object should exist in S3 after creation");
        listBucketContents();

        // Save bundle version #2
        final BundleVersionCoordinate versionCoordinate2 = mock(BundleVersionCoordinate.class);
        when(versionCoordinate2.getBucketId()).thenReturn(bucketId.toString());
        when(versionCoordinate2.getGroupId()).thenReturn("org.apache.nifi");
        when(versionCoordinate2.getArtifactId()).thenReturn("nifi-foo-nar");
        when(versionCoordinate2.getVersion()).thenReturn("2.0.0");
        when(versionCoordinate2.getType()).thenReturn(BundleVersionType.NIFI_NAR);

        final BundlePersistenceContext context2 = mock(BundlePersistenceContext.class);
        when(context2.getCoordinate()).thenReturn(versionCoordinate2);
        when(context2.getSize()).thenReturn(narFile.length());

        LOGGER.info("Creating bundle version 2.0.0...");
        try (final InputStream in = new FileInputStream(narFile)) {
            provider.createBundleVersion(context2, in);
            LOGGER.info("Bundle version 2.0.0 created successfully");
        }

        // Verify object was created in S3
        String key2 = getKeyViaReflection(versionCoordinate2);
        LOGGER.info("Expected key for version 2.0.0: {}", key2);
        assertTrue(objectExists(key2), "Object should exist in S3 after creation");
        listBucketContents();

        // Verify we can retrieve version #1
        LOGGER.info("Retrieving bundle version 1.0.0...");
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        provider.getBundleVersionContent(versionCoordinate1, outputStream);
        assertEquals(narFile.length(), outputStream.size());
        LOGGER.info("Successfully retrieved bundle version 1.0.0");

        // Delete version #1
        LOGGER.info("Deleting bundle version 1.0.0...");
        provider.deleteBundleVersion(versionCoordinate1);
        LOGGER.info("Bundle version 1.0.0 deleted");

        // Verify object was removed from S3
        assertTrue(!objectExists(key1), "Object should not exist in S3 after deletion");
        listBucketContents();

        // Verify we can no longer retrieve version #1
        LOGGER.info("Verifying version 1.0.0 is no longer accessible...");
        final ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
        assertThrows(Exception.class, () -> {
            provider.getBundleVersionContent(versionCoordinate1, outputStream2);
        }, "Should have thrown exception for deleted bundle");
        LOGGER.info("Confirmed version 1.0.0 is no longer accessible");

        // Call delete all bundle versions which should leave an empty bucket
        final BundleCoordinate bundleCoordinate = mock(BundleCoordinate.class);
        when(bundleCoordinate.getBucketId()).thenReturn(bucketId.toString());
        when(bundleCoordinate.getGroupId()).thenReturn("org.apache.nifi");
        when(bundleCoordinate.getArtifactId()).thenReturn("nifi-foo-nar");

        LOGGER.info("Deleting all bundle versions...");
        provider.deleteAllBundleVersions(bundleCoordinate);
        LOGGER.info("All bundle versions deleted");

        // Verify bucket is empty
        listBucketContents();
        ListObjectsResponse finalResponse = s3Client.listObjects(ListObjectsRequest.builder()
                .bucket(bucketName)
                .build());
        assertEquals(0, finalResponse.contents().size(), "Bucket should be empty after deleting all versions");
    }
}