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

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Sets up the credentials required for S3.
 */
public class S3Setup {
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    protected final static String BUCKET_NAME = "test-bucket-00000000-0000-0000-0000-123456789021";
    protected final static String RESOURCE_NAME = "/hello.txt";
    protected final static String REGION = "eu-west-1";

    protected static PropertiesCredentials credentials;
    protected static AmazonS3Client client;

    @BeforeClass
    public static void oneTimeSetup() {
        // Creates a new bucket for this test
        try {
            credentials = new PropertiesCredentials(new FileInputStream(CREDENTIALS_FILE));
            client = new AmazonS3Client(credentials);

            if (client.doesBucketExist(BUCKET_NAME)) {
                Assert.fail("Choose a different bucket name to continue test");
            }

            CreateBucketRequest request = new CreateBucketRequest(BUCKET_NAME, REGION);
            client.createBucket(request);

        } catch (final AmazonS3Exception e) {
            System.err.println("Can't create the key " + BUCKET_NAME + ":" + e.toString());
        } catch (final IOException e) {
            System.err.println(CREDENTIALS_FILE + " doesn't exist.");
        }

        if (!client.doesBucketExist(BUCKET_NAME)) {
            Assert.fail("Setup incomplete, tests will fail");
        }
    }

    @AfterClass
    public static void oneTimeTearDown() {
        // Empty the bucket, before deleting it.
        try {
            ObjectListing objectListing = client.listObjects(BUCKET_NAME);

            while (true) {
                for (Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext(); ) {
                    S3ObjectSummary objectSummary = (S3ObjectSummary) iterator.next();
                    client.deleteObject(BUCKET_NAME, objectSummary.getKey());
                }

                if (objectListing.isTruncated()) {
                    objectListing = client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }

            DeleteBucketRequest dbr = new DeleteBucketRequest(BUCKET_NAME);
            client.deleteBucket(dbr);
        } catch (final AmazonS3Exception e) {
            System.err.println("Unable to delete bucket " + BUCKET_NAME + e.toString());
        }

        if (client.doesBucketExist(BUCKET_NAME)) {
            Assert.fail("Incomplete teardown, subsequent tests might fail");
        }

    }

    protected void uploadTestFile(String key) throws IOException  {
        PropertiesCredentials credentials = new PropertiesCredentials(new FileInputStream(CREDENTIALS_FILE));
        AmazonS3Client client = new AmazonS3Client(credentials);

        PutObjectRequest putRequest = new PutObjectRequest(BUCKET_NAME, key, getFile(RESOURCE_NAME));

        client.putObject(putRequest);
    }

    protected Path getResourcePath(String resourceName) {
        Path path = null;

        try {
            path = Paths.get(getClass().getResource(resourceName).toURI());
        } catch (URISyntaxException e) {
            System.err.println("Resource: " + resourceName + " does not exist" + e.toString());
        }

        return path;
    }

    private File getFile(String resourceName) {
        URI uri = null;
        try {
            uri = this.getClass().getResource(resourceName).toURI();
        } catch (URISyntaxException e) {
            System.err.println("Unable to retrieve File: " + resourceName + " " + e.toString());
            Assert.fail("Cannot proceed without File : " + resourceName);
        }

        return new File(uri);
    }
}