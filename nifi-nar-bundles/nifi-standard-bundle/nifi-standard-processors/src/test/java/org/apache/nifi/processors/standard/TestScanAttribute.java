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

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestScanAttribute {

    @Test
    public void testSingleMatch() {
        final TestRunner runner = TestRunners.newTestRunner(new ScanAttribute());
        runner.setVariable("dictionary", "src/test/resources/ScanAttribute/dictionary1");
        runner.setProperty(ScanAttribute.DICTIONARY_FILE, "${dictionary}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "world");

        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_MATCHED, 1);
        runner.clearTransferState();

        attributes.remove("abc");
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_UNMATCHED, 1);
        runner.clearTransferState();

        attributes.put("abc", "world");
        runner.setProperty(ScanAttribute.ATTRIBUTE_PATTERN, "a.*");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_MATCHED, 1);
        runner.clearTransferState();

        runner.setProperty(ScanAttribute.ATTRIBUTE_PATTERN, "c.*");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_UNMATCHED, 1);
        runner.clearTransferState();

    }

    @Test
    public void testAllMatch() {
        final TestRunner runner = TestRunners.newTestRunner(new ScanAttribute());
        runner.setProperty(ScanAttribute.DICTIONARY_FILE, "src/test/resources/ScanAttribute/dictionary1");
        runner.setProperty(ScanAttribute.MATCHING_CRITERIA, ScanAttribute.MATCH_CRITERIA_ALL);
        runner.setProperty(ScanAttribute.ATTRIBUTE_PATTERN, "a.*");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "world");

        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_MATCHED, 1);
        runner.clearTransferState();

        attributes.remove("abc");
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_MATCHED, 1);
        runner.clearTransferState();

        attributes.put("abc", "world");
        attributes.put("a world", "apart");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_UNMATCHED, 1);
        runner.clearTransferState();

        attributes.put("abc", "world");
        attributes.put("a world", "hello");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_MATCHED, 1);
    }

    @Test
    public void testWithEmptyEntries() {
        final TestRunner runner = TestRunners.newTestRunner(new ScanAttribute());
        runner.setProperty(ScanAttribute.DICTIONARY_FILE, "src/test/resources/ScanAttribute/dictionary-with-empty-new-lines");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "");

        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_UNMATCHED, 1);
        runner.clearTransferState();

        runner.setProperty(ScanAttribute.ATTRIBUTE_PATTERN, "a.*");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_UNMATCHED, 1);
    }

    @Test
    public void testWithDictionaryFilter() {
        final TestRunner runner = TestRunners.newTestRunner(new ScanAttribute());
        runner.setProperty(ScanAttribute.DICTIONARY_FILE, "src/test/resources/ScanAttribute/dictionary-with-extra-info");
        runner.setProperty(ScanAttribute.DICTIONARY_FILTER, "(.*)<greeting>");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "hello");

        runner.enqueue(new byte[0], attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_MATCHED, 1);
        runner.clearTransferState();

        attributes.put("abc", "world");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_UNMATCHED, 1);
        runner.clearTransferState();

        runner.setProperty(ScanAttribute.DICTIONARY_FILTER, "(.*)<.*>");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanAttribute.REL_MATCHED, 1);
        runner.clearTransferState();
    }
}
