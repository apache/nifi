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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("deprecation")
public class TestHashContent {

    @Test
    public void testSHA256() throws IOException {
        test("SHA-256", "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f");
    }

    private void test(final String hashAlgorithm, final String expectedHash) throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new HashContent());
        runner.setProperty(org.apache.nifi.processors.standard.HashContent.ATTRIBUTE_NAME, "hash");
        runner.setProperty(org.apache.nifi.processors.standard.HashContent.HASH_ALGORITHM, hashAlgorithm);

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));

        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.HashContent.REL_SUCCESS, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(org.apache.nifi.processors.standard.HashContent.REL_SUCCESS).get(0);
        final String hashValue = outFile.getAttribute("hash");

        assertEquals(expectedHash, hashValue);
    }
}
