/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.mongodb.gridfs;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeleteGridFSIT extends GridFSITTestBase {
    private TestRunner runner;
    private static final String BUCKET = "delete_test_bucket";

    @Before
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(DeleteGridFS.class);
        super.setup(runner, BUCKET, false);
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testFileAndQueryAtSameTime() {
        runner.setProperty(DeleteGridFS.FILE_NAME, "${test_var}");
        runner.setProperty(DeleteGridFS.QUERY, "{}");
        runner.assertNotValid();
    }

    @Test
    public void testNeitherFileNorQuery() {
        runner.assertNotValid();
    }

    @Test
    public void testDeleteByFileName() {
        testDeleteByProperty(DeleteGridFS.FILE_NAME, String.format("${%s}", CoreAttributes.FILENAME.key()), setupTestFile());
    }

    @Test
    public void testDeleteByQuery() {
        testDeleteByProperty(DeleteGridFS.QUERY, "{}", setupTestFile());
    }

    @Test
    public void testQueryAttribute() {
        String attrName = "gridfs.query.used";
        String fileName = setupTestFile();
        runner.setProperty(DeleteGridFS.QUERY_ATTRIBUTE, attrName);
        testDeleteByProperty(DeleteGridFS.FILE_NAME, String.format("${%s}", CoreAttributes.FILENAME.key()), fileName);
        testForQueryAttribute(fileName, attrName);
    }

    private void testForQueryAttribute(String mustContain, String attrName) {
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeleteGridFS.REL_SUCCESS);
        String attribute = flowFiles.get(0).getAttribute(attrName);
        Assert.assertTrue(attribute.contains(mustContain));
    }

    private String setupTestFile() {
        String fileName = "simple-delete-test.txt";
        ObjectId id = writeTestFile(fileName, "Hello, world!", BUCKET, new HashMap<>());
        Assert.assertNotNull(id);

        return fileName;
    }

    private void testDeleteByProperty(PropertyDescriptor descriptor, String value, String fileName) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), fileName);
        runner.setProperty(descriptor, value);
        runner.assertValid();
        runner.enqueue("test", attrs);
        runner.run();

        runner.assertTransferCount(DeleteGridFS.REL_FAILURE, 0);
        runner.assertTransferCount(DeleteGridFS.REL_SUCCESS, 1);

        Assert.assertFalse(String.format("File %s still exists.", fileName), fileExists(fileName, BUCKET));
    }
}
