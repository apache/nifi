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
package org.apache.nifi.processors.standard;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.apache.nifi.processors.standard.DuplicateFlowFile.COPY_INDEX_ATTRIBUTE;

public class TestDuplicateFlowFile {

    @Test
    public void test() {
        final int numCopies = 100;
        final TestRunner runner = TestRunners.newTestRunner(DuplicateFlowFile.class);
        runner.setProperty(DuplicateFlowFile.NUM_COPIES, Integer.toString(numCopies));

        runner.enqueue("hello".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(DuplicateFlowFile.REL_SUCCESS, numCopies + 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DuplicateFlowFile.REL_SUCCESS);
        // copy.index starts with 1, original has copy.index = 0 but is transferred last
        for (int i = 1; i <= numCopies; i++) {
            flowFiles.get(i - 1).assertAttributeEquals(COPY_INDEX_ATTRIBUTE, Integer.toString(i));
        }
        flowFiles.get(numCopies).assertAttributeEquals(COPY_INDEX_ATTRIBUTE, "0");
    }

    @Test
    public void testNumberOfCopiesEL() {
        final TestRunner runner = TestRunners.newTestRunner(DuplicateFlowFile.class);
        runner.setProperty(DuplicateFlowFile.NUM_COPIES, "${num.copies}");

        runner.enqueue("hello".getBytes(), new HashMap<String, String>() {{
            put("num.copies", "100");
        }});
        runner.run();

        runner.assertAllFlowFilesTransferred(DuplicateFlowFile.REL_SUCCESS, 101);
    }

}
