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
package org.apache.nifi.processors.box;

import com.box.sdk.BoxFile;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * See Javadoc {@link AbstractBoxFileIT} for instructions how to run this test.
 */
public class FetchBoxFileIT extends AbstractBoxFileIT<FetchBoxFile> {
    @Override
    public FetchBoxFile createTestSubject() {
        FetchBoxFile testSubject = new FetchBoxFile();

        return testSubject;
    }

    @Test
    void testFetchSingleFile() throws Exception {
        // GIVEN
        BoxFile.Info file = createFileWithDefaultContent("test_file.txt", mainFolderId);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("box.id", file.getID());
        inputFlowFileAttributes.put("filename", file.getName());

        HashSet<Map<String, String>> expectedAttributes = new HashSet<>(Arrays.asList(inputFlowFileAttributes));
        List<String> expectedContent = Arrays.asList(DEFAULT_FILE_CONTENT);

        // WHEN
        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchBoxFile.REL_FAILURE, 0);

        testRunner.assertAttributes(FetchBoxFile.REL_SUCCESS, getCheckedAttributeNames(), expectedAttributes);
        testRunner.assertContents(FetchBoxFile.REL_SUCCESS, expectedContent);
    }

    @Test
    void testInputFlowFileReferencesMissingFile() throws Exception {
        // GIVEN
        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("box.id", "111");
        inputFlowFileAttributes.put("filename", "missing_filename");

        Set<Map<String, String>> expectedFailureAttributes = new HashSet<>(Arrays.asList(
            new HashMap<String, String>() {{
                put("box.id", "111");
                put("filename", "missing_filename");
                put("error.code", "404");
            }}
        ));

        // WHEN
        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchBoxFile.REL_SUCCESS, 0);

        testRunner.assertAttributes(FetchBoxFile.REL_FAILURE, getCheckedAttributeNames(), expectedFailureAttributes);
    }

    @Test
    void testInputFlowFileThrowsExceptionBeforeFetching() throws Exception {
        // GIVEN
        BoxFile.Info file = createFileWithDefaultContent("test_file.txt", mainFolderId);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("box.id", file.getID());
        inputFlowFileAttributes.put("filename", file.getName());

        MockFlowFile input = new MockFlowFile(1) {
            AtomicBoolean throwException = new AtomicBoolean(true);

            @Override
            public boolean isPenalized() {
                // We want to throw exception only once because the exception handling itself calls this again
                if (throwException.get()) {
                    throwException.set(false);
                    throw new RuntimeException("Intentional exception");
                } else {
                    return super.isPenalized();
                }
            }

            @Override
            public Map<String, String> getAttributes() {
                return inputFlowFileAttributes;
            }
        };

        Set<Map<String, String>> expectedFailureAttributes = new HashSet<>(Arrays.asList(
            new HashMap<String, String>() {{
                putAll(inputFlowFileAttributes);
            }}
        ));

        // WHEN
        testRunner.enqueue(input);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchBoxFile.REL_SUCCESS, 0);

        testRunner.assertAttributes(FetchBoxFile.REL_FAILURE, getCheckedAttributeNames(), expectedFailureAttributes);
    }

    public Set<String> getCheckedAttributeNames() {
        Set<String> checkedAttributeNames = new HashSet<>();

        checkedAttributeNames.add(BoxFlowFileAttribute.ID.getName());
        checkedAttributeNames.add(BoxFlowFileAttribute.FILENAME.getName());
        checkedAttributeNames.add(FetchBoxFile.ERROR_CODE_ATTRIBUTE);

        return checkedAttributeNames;
    }
}
