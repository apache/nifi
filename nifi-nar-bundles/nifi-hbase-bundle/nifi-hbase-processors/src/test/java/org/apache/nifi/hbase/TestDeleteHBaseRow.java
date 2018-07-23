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

package org.apache.nifi.hbase;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDeleteHBaseRow extends DeleteTestBase {

    @Before
    public void setup() throws InitializationException {
        super.setup(DeleteHBaseRow.class);
    }

    @Test
    public void testSimpleDelete() {
        List<String> ids = populateTable(100);

        runner.setProperty(DeleteHBaseRow.BATCH_SIZE, "100");
        runner.setProperty(DeleteHBaseRow.FLOWFILE_FETCH_COUNT, "100");
        for (String id : ids) {
            runner.enqueue(id);
        }

        runner.run(1, true);
        Assert.assertTrue("The mock client was not empty.", hBaseClient.isEmpty());
    }

    private String buildSeparatedString(List<String> ids, String separator) {
        StringBuilder sb = new StringBuilder();
        for (int index = 1; index <= ids.size(); index++) {
            sb.append(ids.get(index - 1)).append(separator);
        }

        return sb.toString();
    }

    private void testSeparatedDeletes(String separator) {
        testSeparatedDeletes(separator, separator, new HashMap());
    }

    private void testSeparatedDeletes(String separator, String separatorProp, Map attrs) {
        List<String> ids = populateTable(10000);
        runner.setProperty(DeleteHBaseRow.KEY_SEPARATOR, separator);
        runner.setProperty(DeleteHBaseRow.BATCH_SIZE, "100");
        runner.enqueue(buildSeparatedString(ids, separatorProp), attrs);
        runner.run(1, true);

        Assert.assertTrue("The mock client was not empty.", hBaseClient.isEmpty());
    }

    @Test
    public void testDeletesSeparatedByNewLines() {
        testSeparatedDeletes("\n");
    }

    @Test
    public void testDeletesSeparatedByCommas() {
        testSeparatedDeletes(",");
    }

    @Test
    public void testDeleteWithELSeparator() {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("test.separator", "____");
        testSeparatedDeletes("${test.separator}", "____", attrs);
    }

    @Test
    public void testDeleteWithExpressionLanguage() {
        List<String> ids = populateTable(1000);
        for (String id : ids) {
            String[] parts = id.split("-");
            Map<String, String> attrs = new HashMap<>();
            for (int index = 0; index < parts.length; index++) {
                attrs.put(String.format("part_%d", index), parts[index]);
            }
            runner.enqueue(id, attrs);
        }
        runner.setProperty(DeleteHBaseRow.ROW_ID, "${part_0}-${part_1}-${part_2}-${part_3}-${part_4}");
        runner.setProperty(DeleteHBaseRow.ROW_ID_LOCATION, DeleteHBaseRow.ROW_ID_ATTR);
        runner.setProperty(DeleteHBaseRow.BATCH_SIZE, "200");
        runner.run(1, true);
    }

    @Test
    public void testConnectivityErrorHandling() {
        List<String> ids = populateTable(100);
        for (String id : ids) {
            runner.enqueue(id);
        }
        boolean exception = false;
        try {
            hBaseClient.setThrowException(true);
            runner.run(1, true);
        } catch (Exception ex) {
            exception = true;
        } finally {
            hBaseClient.setThrowException(false);
        }

        Assert.assertFalse("An unhandled exception was caught.", exception);
    }

    @Test
    public void testRestartIndexAttribute() {
        List<String> ids = populateTable(500);
        StringBuilder sb = new StringBuilder();
        for (int index = 0; index < ids.size(); index++) {
            sb.append(ids.get(index)).append( index < ids.size() - 1 ? "," : "");
        }
        runner.enqueue(sb.toString());
        runner.setProperty(DeleteHBaseRow.ROW_ID_LOCATION, DeleteHBaseRow.ROW_ID_CONTENT);

        Assert.assertTrue("There should have been 500 rows.", hBaseClient.size() == 500);

        hBaseClient.setDeletePoint(20);
        hBaseClient.setThrowExceptionDuringBatchDelete(true);
        runner.run(1, true, true);

        runner.assertTransferCount(DeleteHBaseRow.REL_FAILURE, 1);
        runner.assertTransferCount(DeleteHBaseRow.REL_SUCCESS, 0);

        Assert.assertTrue("Partially deleted", hBaseClient.size() < 500);

        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(DeleteHBaseRow.REL_FAILURE);
        Assert.assertNotNull("Missing restart.index attribute", flowFile.get(0).getAttribute("restart.index"));

        byte[] oldData = runner.getContentAsByteArray(flowFile.get(0));
        Map<String, String> attrs = new HashMap<>();
        attrs.put("restart.index", flowFile.get(0).getAttribute("restart.index"));
        runner.enqueue(oldData, attrs);
        hBaseClient.setDeletePoint(-1);
        hBaseClient.setThrowExceptionDuringBatchDelete(false);
        runner.clearTransferState();
        runner.run(1, true, true);

        runner.assertTransferCount(DeleteHBaseRow.REL_FAILURE, 0);
        runner.assertTransferCount(DeleteHBaseRow.REL_SUCCESS, 1);

        flowFile = runner.getFlowFilesForRelationship(DeleteHBaseRow.REL_SUCCESS);

        Assert.assertTrue("The client should have been empty", hBaseClient.isEmpty());
        Assert.assertNull("The restart.index attribute should be null", flowFile.get(0).getAttribute("restart.index"));

    }
}
