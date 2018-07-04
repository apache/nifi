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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.gcp.GCPIntegrationTests;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

import static org.junit.Assert.fail;

/**
 * Base class for GCS Integration Tests. Establishes a bucket and helper methods for creating test scenarios.
 * Assumes use of <a href=https://developers.google.com/identity/protocols/application-default-credentials">Application Default</a>
 * credentials for running tests.
 */
@Category(GCPIntegrationTests.class)
public abstract class AbstractGCSIT {
    private static final String PROJECT_ID = System.getProperty("test.gcp.project.id", "nifi-test-gcp-project");
    protected static final String BUCKET = RemoteStorageHelper.generateBucketName();
    protected static final String ENCRYPTION_KEY = "3gCN8OOPAGpDwRYieHAj6fR0eBSG5vloaHl9vlZ3doQ=";
    protected static final Integer RETRIES = 6;

    protected static RemoteStorageHelper helper;
    protected static Storage storage;

    @BeforeClass
    public static void setUp() {
        try {
            helper = RemoteStorageHelper.create();
            storage = helper.getOptions().getService();

            if (storage.get(BUCKET) != null) {
                // As the generateBucketName function uses a UUID, this should pretty much never happen
                fail("Bucket " + BUCKET + " exists. Please rerun the test to generate a new bucket name.");
            }

            // Create the bucket
            storage.create(BucketInfo.of(BUCKET));
        } catch (StorageException e) {
            fail("Can't create bucket " + BUCKET + ": " + e.getLocalizedMessage());
        }

        if (storage.get(BUCKET) == null) {
            fail("Setup incomplete, tests will fail");
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            // Empty the bucket before deleting it.
            Iterable<Blob> blobIterable = storage.list(BUCKET, Storage.BlobListOption.versions(true)).iterateAll();

            for (final Blob blob : blobIterable) {
                storage.delete(blob.getBlobId());
            }

            storage.delete(BUCKET);
        } catch (final StorageException e) {
            fail("Unable to delete bucket " + BUCKET + ": " + e.getLocalizedMessage());
        }

        if (storage.get(BUCKET) != null) {
            fail("Incomplete teardown, subsequent tests might fail");
        }
    }

    protected static TestRunner buildNewRunner(Processor processor) throws Exception {
        final GCPCredentialsControllerService credentialsControllerService = new GCPCredentialsControllerService();
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("gcpCredentialsControllerService", credentialsControllerService);
        runner.enableControllerService(credentialsControllerService);

        runner.setProperty(AbstractGCSProcessor.GCP_CREDENTIALS_PROVIDER_SERVICE, "gcpCredentialsControllerService");
        runner.setProperty(AbstractGCSProcessor.PROJECT_ID, PROJECT_ID);
        runner.setProperty(AbstractGCSProcessor.RETRY_COUNT, String.valueOf(RETRIES));

        runner.assertValid(credentialsControllerService);

        return runner;
    }

    /**
     * Puts a test file onto Google Cloud Storage in bucket {@link AbstractGCSIT#BUCKET}.
     *
     * @param key Key which the file will be uploaded under
     * @param bytes The content of the file to be uploaded
     * @throws StorageException if the file can't be created for some reason
     */
    protected void putTestFile(String key, byte[] bytes) throws StorageException {
        storage.create(BlobInfo.newBuilder(BlobId.of(BUCKET, key))
                .build(), bytes
        );
    }

    /**
     *  Puts a test file onto Google Cloud Storage in bucket {@link AbstractGCSIT#BUCKET}. This file is encrypted with
     *  server-side encryption using {@link AbstractGCSIT#ENCRYPTION_KEY}.
     *
     * @param key Key which the file will be uploaded under
     * @param bytes The content of the file to be uploaded
     * @throws StorageException if the file can't be created for some reason
     */
    protected void putTestFileEncrypted(String key, byte[] bytes) throws StorageException {
        storage.create(BlobInfo.newBuilder(BlobId.of(BUCKET, key))
        .build(), bytes, Storage.BlobTargetOption.encryptionKey(ENCRYPTION_KEY));
    }

    /**
     * Test if the file exists in Google Cloud Storage in bucket {@link AbstractGCSIT#BUCKET}.
     *
     * @param key Key to check for the file
     * @return true if the file exists, false if it doesn't
     * @throws StorageException if there are any issues accessing the file or connecting to GCS.
     */
    protected boolean fileExists(String key) throws StorageException {
        return (storage.get(BlobId.of(BUCKET, key)) != null);
    }

    /**
     * Test if the file exists in Google Cloud Storage in bucket {@link AbstractGCSIT#BUCKET}, and if the content is as
     * specified.
     *
     * @param key Key to check for the file
     * @param bytes The content to compare to the content of the file
     * @return true if the file exists and the content of the file is equal to {@code bytes}, false otherwise.
     * @throws StorageException if there are any issues accessing the file or connecting to GCS.
     */
    protected boolean fileEquals(String key, byte[] bytes) throws StorageException {
        return (fileExists(key) && Arrays.equals(storage.readAllBytes(BlobId.of(BUCKET, key)), bytes));
    }

    /**
     * Test if the file exists in Google Cloud Storage in bucket {@link AbstractGCSIT#BUCKET}, and if the content is as
     * specified. Assumes that the file is encrypted using {@link AbstractGCSIT#ENCRYPTION_KEY}.
     *
     * @param key Key to check for the file
     * @param bytes The content to compare to the content of the file
     * @return true if the file exists and the content of the file is equal to {@code bytes}, false otherwise.
     * @throws StorageException if there are any issues accessing the file or connecting to GCS.
     */
    protected boolean fileEqualsEncrypted(String key, byte[] bytes) throws StorageException {
        return (fileExists(key) && Arrays.equals(
                storage.readAllBytes(BlobId.of(BUCKET, key), Storage.BlobSourceOption.decryptionKey(ENCRYPTION_KEY)),
                bytes));
    }
}
