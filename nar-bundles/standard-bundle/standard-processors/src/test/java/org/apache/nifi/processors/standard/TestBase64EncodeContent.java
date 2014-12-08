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

import org.apache.nifi.processors.standard.Base64EncodeContent;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

public class TestBase64EncodeContent {

    @Test
    public void testRoundTrip() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new Base64EncodeContent());

        testRunner.setProperty(Base64EncodeContent.MODE, Base64EncodeContent.ENCODE_MODE);

        testRunner.enqueue(Paths.get("src/test/resources/hello.txt"));
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(Base64EncodeContent.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(Base64EncodeContent.REL_SUCCESS).get(0);
        testRunner.assertQueueEmpty();

        testRunner.setProperty(Base64EncodeContent.MODE, Base64EncodeContent.DECODE_MODE);
        testRunner.enqueue(flowFile);
        testRunner.clearTransferState();
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(Base64EncodeContent.REL_SUCCESS, 1);

        flowFile = testRunner.getFlowFilesForRelationship(Base64EncodeContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(new File("src/test/resources/hello.txt"));
    }

    @Test
    public void testFailDecodeNotBase64() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new Base64EncodeContent());

        testRunner.setProperty(Base64EncodeContent.MODE, Base64EncodeContent.DECODE_MODE);

        testRunner.enqueue(Paths.get("src/test/resources/hello.txt"));
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(Base64EncodeContent.REL_FAILURE, 1);
    }

    @Test
    public void testFailDecodeNotBase64ButIsAMultipleOfFourBytes() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new Base64EncodeContent());

        testRunner.setProperty(Base64EncodeContent.MODE, Base64EncodeContent.DECODE_MODE);

        testRunner.enqueue("four@@@@multiple".getBytes());
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(Base64EncodeContent.REL_FAILURE, 1);
    }

}
