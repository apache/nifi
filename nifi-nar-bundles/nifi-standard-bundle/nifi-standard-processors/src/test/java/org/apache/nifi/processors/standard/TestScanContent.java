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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

public class TestScanContent {

    @Test
    public void testBlankLineInDictionaryTextEncoding() throws IOException {
        final String dictionaryWithBlankLine = "Line1\n\nLine3";
        final byte[] dictionaryBytes = dictionaryWithBlankLine.getBytes(ScanContent.UTF8);
        final Path dictionaryPath = Paths.get("target/dictionary");
        Files.write(dictionaryPath, dictionaryBytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        final TestRunner runner = TestRunners.newTestRunner(new ScanContent());
        runner.setThreadCount(1);
        runner.setProperty(ScanContent.DICTIONARY, dictionaryPath.toString());
        runner.setProperty(ScanContent.DICTIONARY_ENCODING, ScanContent.TEXT_ENCODING);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(ScanContent.REL_NO_MATCH, 1);

    }

    @Ignore("This test has a race condition/ordering problem")
    @Test
    public void testBinaryScan() throws IOException {
        // Create dictionary file.
        final String[] terms = new String[]{"hello", "good-bye"};
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final DataOutputStream dictionaryOut = new DataOutputStream(baos)) {
            for (final String term : terms) {
                final byte[] termBytes = term.getBytes("UTF-8");
                dictionaryOut.writeInt(termBytes.length);
                dictionaryOut.write(termBytes);
            }
            final byte[] termBytes = baos.toByteArray();

            final Path dictionaryPath = Paths.get("target/dictionary");
            Files.write(dictionaryPath, termBytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

            final TestRunner runner = TestRunners.newTestRunner(new ScanContent());
            runner.setThreadCount(1);
            runner.setProperty(ScanContent.DICTIONARY, dictionaryPath.toString());
            runner.setProperty(ScanContent.DICTIONARY_ENCODING, ScanContent.BINARY_ENCODING);

            runner.enqueue(Paths.get("src/test/resources/TestScanContent/helloWorld"));
            runner.enqueue(Paths.get("src/test/resources/TestScanContent/wellthengood-bye"));
            runner.enqueue(new byte[0]);

            while (!runner.isQueueEmpty()) {
                runner.run(3);
                try {  //must insert this delay or flowfiles are made so close together they become out of order in the queue
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    //moving on
                }
            }

            runner.assertTransferCount(ScanContent.REL_MATCH, 2);
            runner.assertTransferCount(ScanContent.REL_NO_MATCH, 1);
            final List<MockFlowFile> matched = runner.getFlowFilesForRelationship(ScanContent.REL_MATCH);
            final List<MockFlowFile> unmatched = runner.getFlowFilesForRelationship(ScanContent.REL_NO_MATCH);

            matched.get(0).assertAttributeEquals(ScanContent.MATCH_ATTRIBUTE_KEY, "hello");
            matched.get(1).assertAttributeEquals(ScanContent.MATCH_ATTRIBUTE_KEY, "good-bye");

            unmatched.get(0).assertAttributeNotExists(ScanContent.MATCH_ATTRIBUTE_KEY);
        }
    }

}
