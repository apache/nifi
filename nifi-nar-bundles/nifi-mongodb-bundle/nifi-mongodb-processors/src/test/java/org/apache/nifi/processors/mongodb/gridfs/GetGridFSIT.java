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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.mongodb.QueryHelper;
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

public class GetGridFSIT extends ITTestBase {
    TestRunner runner;

    static final String BUCKET = "get_test_bucket";

    @Before
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(GetGridFS.class);
        super.setup(runner, BUCKET, false);
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testGetOneByName() {
        final String fileName = "get_by_name.txt";
        final String content  = "Hello, world";
        ObjectId id = writeTestFile(fileName, content, BUCKET, new HashMap<>());
        Assert.assertNotNull(id);

        String query = String.format("{\"filename\": \"%s\"}", fileName);
        runner.enqueue(query);
        runner.run();
        runner.assertTransferCount(GetGridFS.REL_FAILURE, 0);
        runner.assertTransferCount(GetGridFS.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetGridFS.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetGridFS.REL_SUCCESS);
        byte[] rawData = runner.getContentAsByteArray(flowFiles.get(0));
        Assert.assertEquals("Data did not match for the file", new String(rawData), content);

        runner.clearTransferState();
        runner.setProperty(GetGridFS.QUERY, query);
        runner.enqueue("test");
        runner.run();

        runner.assertTransferCount(GetGridFS.REL_FAILURE, 0);
        runner.assertTransferCount(GetGridFS.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetGridFS.REL_SUCCESS, 1);
        flowFiles = runner.getFlowFilesForRelationship(GetGridFS.REL_SUCCESS);
        rawData = runner.getContentAsByteArray(flowFiles.get(0));
        Assert.assertEquals("Data did not match for the file", new String(rawData), content);
    }

    @Test
    public void testGetMany() {
        String baseName = "test_file_%d.txt";
        String content  = "Hello, world take %d";
        for (int index = 0; index < 5; index++) {
            ObjectId id = writeTestFile(String.format(baseName, index), String.format(content, index), BUCKET, new HashMap<>());
            Assert.assertNotNull(id);
        }

        AllowableValue[] values = new AllowableValue[] { QueryHelper.MODE_MANY_COMMITS, QueryHelper.MODE_ONE_COMMIT };

        for (AllowableValue value : values) {
            String query = "{}";
            runner.setProperty(GetGridFS.OPERATION_MODE, value);
            runner.enqueue(query);
            runner.run();

            runner.assertTransferCount(GetGridFS.REL_FAILURE, 0);
            runner.assertTransferCount(GetGridFS.REL_ORIGINAL, 1);
            runner.assertTransferCount(GetGridFS.REL_SUCCESS, 5);
            runner.clearTransferState();
        }
    }

    @Test
    public void testQueryAttribute() {
        final String fileName = "get_by_name.txt";
        final String content  = "Hello, world";
        ObjectId id = writeTestFile(fileName, content, BUCKET, new HashMap<>());
        Assert.assertNotNull(id);

        final String queryAttr = "gridfs.query.used";
        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), fileName);
        runner.setProperty(GetGridFS.FILE_NAME, String.format("${%s}", CoreAttributes.FILENAME.key()));
        runner.setProperty(GetGridFS.QUERY_ATTRIBUTE, queryAttr);
        runner.enqueue(content, attrs);
        runner.run();

        runner.assertTransferCount(GetGridFS.REL_FAILURE, 0);
        runner.assertTransferCount(GetGridFS.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetGridFS.REL_SUCCESS, 1);
        MockFlowFile mff = runner.getFlowFilesForRelationship(GetGridFS.REL_SUCCESS).get(0);
        String attr = mff.getAttribute(queryAttr);
        Assert.assertNotNull("Query attribute was null.", attr);
        Assert.assertTrue("Wrong content.", attr.contains("filename"));

        runner.clearTransferState();

        id = writeTestFile(fileName, content, BUCKET, new HashMap<String, Object>(){{
            put("lookupKey", "xyz");
        }});
        Assert.assertNotNull(id);

        String query = "{ \"metadata\": { \"lookupKey\": \"xyz\" }}";

        runner.removeProperty(GetGridFS.FILE_NAME);
        runner.setProperty(GetGridFS.QUERY, query);
        runner.enqueue(content, attrs);
        runner.run();
        runner.assertTransferCount(GetGridFS.REL_FAILURE, 0);
        runner.assertTransferCount(GetGridFS.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetGridFS.REL_SUCCESS, 1);
        mff = runner.getFlowFilesForRelationship(GetGridFS.REL_SUCCESS).get(0);
        attr = mff.getAttribute(queryAttr);
        Assert.assertNotNull("Query attribute was null.", attr);
        Assert.assertTrue("Wrong content.", attr.contains("metadata"));
    }

    @Test
    public void testGetQueryFromBody() {
        runner.enqueue("{}");
        testQueryFromSource(0, 1, 1);
    }

    @Test
    public void testGetQueryFromQueryParam() {
        runner.setProperty(GetGridFS.QUERY, "{}");
        runner.enqueue("");
        testQueryFromSource(0, 1, 1);
    }

    @Test
    public void testGetQueryFromFileNameParam() {
        Map<String, String> attr = new HashMap<>();
        attr.put(CoreAttributes.FILENAME.key(), "get_by_name.txt");
        runner.setProperty(GetGridFS.FILE_NAME, String.format("${%s}", CoreAttributes.FILENAME.key()));
        runner.enqueue("test", attr);
        testQueryFromSource(0, 1, 1);
    }

    private void testQueryFromSource(int failure, int original, int success) {
        final String fileName = "get_by_name.txt";
        final String content  = "Hello, world";
        ObjectId id = writeTestFile(fileName, content, BUCKET, new HashMap<>());
        Assert.assertNotNull(id);

        runner.run();
        runner.assertTransferCount(GetGridFS.REL_FAILURE, failure);
        runner.assertTransferCount(GetGridFS.REL_ORIGINAL, original);
        runner.assertTransferCount(GetGridFS.REL_SUCCESS, success);
    }
}
