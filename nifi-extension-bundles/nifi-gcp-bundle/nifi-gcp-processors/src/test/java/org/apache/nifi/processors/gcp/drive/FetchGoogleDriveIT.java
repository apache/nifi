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
package org.apache.nifi.processors.gcp.drive;

import com.google.api.services.drive.model.File;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.valueOf;
import static java.util.Collections.singletonList;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_CODE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MIME_TYPE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE_AVAILABLE;

/**
 * See Javadoc {@link AbstractGoogleDriveIT} for instructions how to run this test.
 */
public class FetchGoogleDriveIT extends AbstractGoogleDriveIT<FetchGoogleDrive> implements OutputChecker {
    @Override
    public FetchGoogleDrive createTestSubject() {
        FetchGoogleDrive testSubject = new FetchGoogleDrive();

        return testSubject;
    }

    @Test
    void testFetch() throws Exception {
        File file = createFileWithDefaultContent("test_file.txt", mainFolderId);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put(GoogleDriveAttributes.ID, file.getId());
        inputFlowFileAttributes.put(GoogleDriveAttributes.FILENAME, file.getName());
        inputFlowFileAttributes.put(GoogleDriveAttributes.SIZE, valueOf(DEFAULT_FILE_CONTENT.length()));
        inputFlowFileAttributes.put(GoogleDriveAttributes.SIZE_AVAILABLE, "true");
        inputFlowFileAttributes.put(GoogleDriveAttributes.MIME_TYPE, "text/plain");

        Set<Map<String, String>> expectedAttributes = new HashSet<>(singletonList(inputFlowFileAttributes));
        List<String> expectedContent = singletonList(DEFAULT_FILE_CONTENT);

        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        testRunner.assertTransferCount(FetchGoogleDrive.REL_FAILURE, 0);

        checkAttributes(FetchGoogleDrive.REL_SUCCESS, expectedAttributes);
        checkContent(FetchGoogleDrive.REL_SUCCESS, expectedContent);
    }

    @Test
    void testInputFlowFileReferencesMissingFile() {
        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put(GoogleDriveAttributes.ID, "missing");
        inputFlowFileAttributes.put(GoogleDriveAttributes.FILENAME, "missing_filename");

        Set<Map<String, String>> expectedFailureAttributes = new HashSet<>(singletonList(
                Map.of(GoogleDriveAttributes.ID, "missing",
                    GoogleDriveAttributes.FILENAME, "missing_filename",
                    GoogleDriveAttributes.ERROR_CODE, "404")
        ));

        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        testRunner.assertTransferCount(FetchGoogleDrive.REL_SUCCESS, 0);
        checkAttributes(FetchGoogleDrive.REL_FAILURE, expectedFailureAttributes);
    }

    @Test
    void testInputFlowFileThrowsExceptionBeforeFetching() throws Exception {
        File file = createFileWithDefaultContent("test_file.txt", mainFolderId);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put(GoogleDriveAttributes.ID, file.getId());
        inputFlowFileAttributes.put(GoogleDriveAttributes.FILENAME, file.getName());
        MockFlowFile input = new MockFlowFile(1) {
            final AtomicBoolean throwException = new AtomicBoolean(true);

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

        Set<Map<String, String>> expectedFailureAttributes = new HashSet<>(singletonList(
                inputFlowFileAttributes
        ));

        testRunner.enqueue(input);
        testRunner.run();

        testRunner.assertTransferCount(FetchGoogleDrive.REL_SUCCESS, 0);

        checkAttributes(FetchGoogleDrive.REL_FAILURE, expectedFailureAttributes);
    }

    @Override
    public Set<String> getCheckedAttributeNames() {
        return Set.of(ID, FILENAME, SIZE, SIZE_AVAILABLE, MIME_TYPE, ERROR_CODE);
    }
}
