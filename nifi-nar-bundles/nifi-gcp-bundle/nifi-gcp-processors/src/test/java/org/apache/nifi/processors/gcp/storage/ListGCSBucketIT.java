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

import com.google.cloud.storage.BucketInfo;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNotEquals;

/**
 * Integration tests for {@link ListGCSBucket} which actually use Google Cloud resources.
 */
public class ListGCSBucketIT extends AbstractGCSIT {
    private static final byte[] CONTENT = {12, 13, 14};

    @Test
    public void testSimpleList() throws Exception {
        putTestFile("a", CONTENT);
        putTestFile("b/c", CONTENT);
        putTestFile("d/e", CONTENT);

        final TestRunner runner = buildNewRunner(new ListGCSBucket());
        runner.setProperty(ListGCSBucket.BUCKET, BUCKET);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "a");
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
    }


    @Test
    public void testSimpleListWithPrefix() throws Exception {
        putTestFile("a", CONTENT);
        putTestFile("b/c", CONTENT);
        putTestFile("d/e", CONTENT);

        final TestRunner runner = buildNewRunner(new ListGCSBucket());
        runner.setProperty(ListGCSBucket.BUCKET, BUCKET);

        runner.setProperty(ListGCSBucket.PREFIX, "b/");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "b/c");
    }



    @Test
    public void testSimpleListWithPrefixAndGenerations() throws Exception {
        // enable versioning
        storage.update(BucketInfo.newBuilder(BUCKET).setVersioningEnabled(true).build());

        putTestFile("generations/a", CONTENT);
        putTestFile("generations/a", CONTENT);
        putTestFile("generations/b", CONTENT);
        putTestFile("generations/c", CONTENT);

        final TestRunner runner = buildNewRunner(new ListGCSBucket());
        runner.setProperty(ListGCSBucket.BUCKET, BUCKET);

        runner.setProperty(ListGCSBucket.PREFIX, "generations/");
        runner.setProperty(ListGCSBucket.USE_GENERATIONS, "true");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS, 4);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "generations/a");
        flowFiles.get(1).assertAttributeEquals("filename", "generations/a");
        flowFiles.get(2).assertAttributeEquals("filename", "generations/b");
        flowFiles.get(3).assertAttributeEquals("filename", "generations/c");

        assertNotEquals(
                flowFiles.get(0).getAttribute(StorageAttributes.GENERATION_ATTR),
                flowFiles.get(1).getAttribute(StorageAttributes.GENERATION_ATTR)
        );
    }


    @Test
    public void testCheckpointing() throws Exception {
        putTestFile("checkpoint/a", CONTENT);
        putTestFile("checkpoint/b/c", CONTENT);

        final TestRunner runner = buildNewRunner(new ListGCSBucket());
        runner.setProperty(ListGCSBucket.BUCKET, BUCKET);

        runner.setProperty(ListGCSBucket.PREFIX, "checkpoint/");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS, 2);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "checkpoint/a");
        flowFiles.get(1).assertAttributeEquals("filename", "checkpoint/b/c");

        putTestFile("checkpoint/d/e", CONTENT);
        runner.run();

        // Should only retrieve 1 new file (for a total of 3)
        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS, 3);
        flowFiles = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "checkpoint/a");
        flowFiles.get(1).assertAttributeEquals("filename", "checkpoint/b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "checkpoint/d/e");
    }
}
