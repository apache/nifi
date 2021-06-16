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
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestDeleteHBaseCells extends DeleteTestBase {

    @Before
    public void setup() throws InitializationException {
        super.setup(DeleteHBaseCells.class);
    }

    @Test
    public void testSimpleDelete() {
        final String SEP = "::::";
        List<String> ids = populateTable(10000);
        runner.setProperty(DeleteHBaseCells.SEPARATOR, SEP);
        runner.assertValid();
        StringBuilder sb = new StringBuilder();
        for (String id : ids) {
            sb.append(String.format("%s%sX%sY\n", id, SEP, SEP));
        }
        runner.enqueue(sb.toString().trim());
        runner.run();
        runner.assertAllFlowFilesTransferred(DeleteHBaseCells.REL_SUCCESS);
    }

    @Test
    public void testWrongNumberOfInputs() {
        final String SEP = "::::";
        List<String> ids = populateTable(10000);
        runner.setProperty(DeleteHBaseCells.SEPARATOR, SEP);
        runner.assertValid();
        StringBuilder sb = new StringBuilder();
        for (String id : ids) {
            sb.append(String.format("%s%sX\n", id, SEP));
        }
        runner.enqueue(sb.toString().trim());
        runner.run();
        runner.assertAllFlowFilesTransferred(DeleteHBaseCells.REL_FAILURE);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(DeleteHBaseCells.REL_FAILURE).get(0);
        flowFile.assertAttributeEquals(DeleteHBaseCells.ERROR_MSG, "Invalid line length. It must have 3 or 4 components. It had 2.");
    }
}
