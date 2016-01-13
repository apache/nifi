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

import com.amazonaws.services.s3.model.StorageClass;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides integration level testing with actual AWS S3 resources for {@link PutS3Object} and requires additional configuration and resources to work.
 */
public class ITPutS3Object extends AbstractS3IT {

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
    public void testPutInFolder() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
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

        runner.setProperty(PutS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
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
}