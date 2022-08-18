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
package org.apache.nifi.processors.smb;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.apache.nifi.processors.smb.FetchSmb.RECORD_READER;
import static org.apache.nifi.processors.smb.FetchSmb.REL_FAILURE;
import static org.apache.nifi.processors.smb.FetchSmb.REL_SUCCESS;
import static org.apache.nifi.processors.smb.FetchSmb.REMOTE_FILE;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.services.smb.SmbjClientProviderService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

public class FetchSmbIT extends SambaTestcontinerIT {

    @Test
    public void fetchFilesUsingEL() throws Exception {
        writeFile("/test_file", "test_content");
        TestRunner testRunner = newTestRunner(FetchSmb.class);
        SmbjClientProviderService smbjClientProviderService = configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.setProperty(REMOTE_FILE, "${attribute_to_find_using_EL}");
        testRunner.enableControllerService(smbjClientProviderService);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute_to_find_using_EL", "test_file");

        testRunner.enqueue("ignored", attributes);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        assertEquals("test_content", testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getContent());
        testRunner.assertValid();
    }

    @Test
    public void fetchFilesUsingRecordReader() throws Exception {
        writeFile("/test_file1", "test_content_1");
        writeFile("/test_file2", "test_content_2");
        TestRunner testRunner = newTestRunner(FetchSmb.class);
        SmbjClientProviderService smbjClientProviderService = configureTestRunnerForSambaDockerContainer(testRunner);
        JsonTreeReader jsonTreeReader = new JsonTreeReader();
        testRunner.addControllerService("record-reader", jsonTreeReader);
        testRunner.setProperty(RECORD_READER, "record-reader");
        testRunner.enableControllerService(jsonTreeReader);
        testRunner.enableControllerService(smbjClientProviderService);

        testRunner.enqueue("{\"identifier\": \"test_file1\"}\n{\"identifier\": \"/test_file2\"}\n");
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 2);
        assertEquals(new HashSet<>(asList("test_content_1", "test_content_2")),
                testRunner.getFlowFilesForRelationship(REL_SUCCESS).stream().map(
                        MockFlowFile::getContent).collect(toSet()));
        testRunner.assertValid();
    }

    @Test
    public void tryToFetchNonExistingFileEmitsFailure() throws Exception {
        TestRunner testRunner = newTestRunner(FetchSmb.class);
        SmbjClientProviderService smbjClientProviderService = configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.setProperty(REMOTE_FILE, "${attribute_to_find_using_EL}");
        testRunner.enableControllerService(smbjClientProviderService);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute_to_find_using_EL", "non_existing_file");

        testRunner.enqueue("ignored", attributes);
        testRunner.run();
        testRunner.assertTransferCount(REL_FAILURE, 1);
    }

}
