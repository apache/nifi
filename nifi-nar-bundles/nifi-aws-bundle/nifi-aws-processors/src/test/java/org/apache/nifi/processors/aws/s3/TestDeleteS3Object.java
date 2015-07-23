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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;


@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class TestDeleteS3Object {

    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    // When you want to test this, you should create a bucket on Amazon S3 as follows.
    private static final String TEST_REGION = "ap-northeast-1";
    private static final String TEST_BUCKET = "test-bucket-00000000-0000-0000-0000-1234567890123";

    @BeforeClass
    public static void oneTimeSetUp() {
        // Creates a new bucket for this test
        try {
            PropertiesCredentials credentials = new PropertiesCredentials(new FileInputStream(CREDENTIALS_FILE));
            AmazonS3Client client = new AmazonS3Client(credentials);
            CreateBucketRequest request = new CreateBucketRequest(TEST_BUCKET, TEST_REGION);
            client.createBucket(request);
        } catch (final AmazonS3Exception e) {
            System.out.println(TEST_BUCKET + " already exists.");
        } catch (final IOException e) {
            System.out.println(CREDENTIALS_FILE + " doesn't exist.");
        }
    }

    @AfterClass
    public static void oneTimeTearDown() throws IOException {
        // Delete a bucket for this test
        PropertiesCredentials credentials = new PropertiesCredentials(new FileInputStream(CREDENTIALS_FILE));
        AmazonS3Client client = new AmazonS3Client(credentials);
        DeleteBucketRequest dbr = new DeleteBucketRequest(TEST_BUCKET);
        client.deleteBucket(dbr);
    }

    @Test
    public void testSimpleDelete() throws IOException {
        // Prepares for this test
        uploadTestFile("hello.txt");

        DeleteS3Object deleter = new DeleteS3Object();
        final TestRunner runner = TestRunners.newTestRunner(deleter);
        runner.setProperty(DeleteS3Object.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(DeleteS3Object.REGION, TEST_REGION);
        runner.setProperty(DeleteS3Object.BUCKET, TEST_BUCKET);
        runner.setProperty(DeleteS3Object.KEY, "hello.txt");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "hello.txt");
        runner.enqueue(new byte[0], attrs);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testDeleteFolder() throws IOException {
        // Prepares for this test
        uploadTestFile("folder/1.txt");

        DeleteS3Object deleter = new DeleteS3Object();
        final TestRunner runner = TestRunners.newTestRunner(deleter);
        runner.setProperty(DeleteS3Object.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(DeleteS3Object.REGION, TEST_REGION);
        runner.setProperty(DeleteS3Object.BUCKET, TEST_BUCKET);
        runner.setProperty(DeleteS3Object.KEY, "folder/1.txt");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "hello.txt");
        runner.enqueue(new byte[0], attrs);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testTryToDeleteNotExistingFile() throws IOException {
        DeleteS3Object deleter = new DeleteS3Object();
        final TestRunner runner = TestRunners.newTestRunner(deleter);
        runner.setProperty(DeleteS3Object.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(DeleteS3Object.REGION, TEST_REGION);
        runner.setProperty(DeleteS3Object.BUCKET, TEST_BUCKET);
        runner.setProperty(DeleteS3Object.BUCKET, "no-such-a-key");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "no-such-a-file");
        runner.enqueue(new byte[0], attrs);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);
    }

    // Uploads a test file
    private void uploadTestFile(String key) throws IOException {
        PropertiesCredentials credentials = new PropertiesCredentials(new FileInputStream(CREDENTIALS_FILE));
        AmazonS3Client client = new AmazonS3Client(credentials);
        URL fileURL = this.getClass().getClassLoader().getResource("hello.txt");
        File file = new File(fileURL.getPath());
        PutObjectRequest putRequest = new PutObjectRequest(TEST_BUCKET, key, file);
        PutObjectResult result = client.putObject(putRequest);
    }
}
