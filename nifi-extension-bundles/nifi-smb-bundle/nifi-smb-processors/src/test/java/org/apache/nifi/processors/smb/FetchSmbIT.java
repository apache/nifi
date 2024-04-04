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

import static org.apache.nifi.processors.smb.FetchSmb.REL_FAILURE;
import static org.apache.nifi.processors.smb.FetchSmb.REL_SUCCESS;
import static org.apache.nifi.processors.smb.FetchSmb.REMOTE_FILE;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.services.smb.SmbjClientProviderService;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

public class FetchSmbIT extends SambaTestContainers {

    @Test
    public void fetchFilesUsingEL() throws Exception {
        writeFile("/test_file", "test_content");
        TestRunner testRunner = newTestRunner(FetchSmb.class);
        testRunner.setProperty(REMOTE_FILE, "${attribute_to_find_using_EL}");
        final SmbjClientProviderService smbjClientProviderService = configureSmbClient(testRunner, true);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute_to_find_using_EL", "test_file");

        testRunner.enqueue("ignored", attributes);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        assertEquals("test_content", testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getContent());
        testRunner.assertValid();
        testRunner.disableControllerService(smbjClientProviderService);
    }

    @Test
    public void tryToFetchNonExistingFileEmitsFailure() throws Exception {
        TestRunner testRunner = newTestRunner(FetchSmb.class);
        testRunner.setProperty(REMOTE_FILE, "${attribute_to_find_using_EL}");
        final SmbjClientProviderService smbjClientProviderService = configureSmbClient(testRunner, true);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute_to_find_using_EL", "non_existing_file");

        testRunner.enqueue("ignored", attributes);
        testRunner.run();
        testRunner.assertTransferCount(REL_FAILURE, 1);
        testRunner.assertValid();
        testRunner.disableControllerService(smbjClientProviderService);
    }

}
