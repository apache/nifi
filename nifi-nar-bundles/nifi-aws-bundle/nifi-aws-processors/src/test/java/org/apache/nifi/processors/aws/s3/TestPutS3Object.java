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
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.services.s3.model.StorageClass;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class TestPutS3Object extends AbstractS3Test {

    @Test
    public void testSimplePut() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        runner.setProperty(PutS3Object.CREDENTAILS_FILE, CREDENTIALS_FILE);
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
    public void testPutInFolder() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        runner.setProperty(PutS3Object.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);

        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/1.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testStorageClass() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        runner.setProperty(PutS3Object.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.REGION, REGION);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.STORAGE_CLASS, StorageClass.ReducedRedundancy.name());

        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/2.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testPermissions() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        runner.setProperty(PutS3Object.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.BUCKET, BUCKET_NAME);
        runner.setProperty(PutS3Object.FULL_CONTROL_USER_LIST,"28545acd76c35c7e91f8409b95fd1aa0c0914bfa1ac60975d9f48bc3c5e090b5");
        runner.setProperty(PutS3Object.REGION, REGION);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/4.txt");
        runner.enqueue(getResourcePath(SAMPLE_FILE_RESOURCE_NAME), attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
    }
}