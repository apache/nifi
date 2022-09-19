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

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFolder;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ListBoxFileTestRunnerTest implements SimpleListBoxFileTestTrait {
    private ListBoxFile testSubject;
    private TestRunner testRunner;

    private BoxAPIConnection mockBoxAPIConnection;

    private BoxFolder mockBoxFolder;

    private String folderId = "folderId";

    @BeforeEach
    void setUp() throws Exception {
        mockBoxAPIConnection = mock(BoxAPIConnection.class);
        mockBoxFolder = mock(BoxFolder.class);

        testSubject = new ListBoxFile() {
            @Override
            BoxFolder getFolder(String folderId) {
                return mockBoxFolder;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        testRunner.setProperty(ListBoxFile.FOLDER_ID, folderId);

        BoxClientService boxClientService = mock(BoxClientService.class);
        doReturn(boxClientService.toString()).when(boxClientService).getIdentifier();
        doReturn(mockBoxAPIConnection).when(boxClientService).getBoxApiConnection();

        testRunner.addControllerService(boxClientService.getIdentifier(), boxClientService);
        testRunner.enableControllerService(boxClientService);
        testRunner.setProperty(BoxClientService.BOX_CLIENT_SERVICE, boxClientService.getIdentifier());
    }

    @Test
    void testOutputAsAttributesWhereTimestampIsModifiedTime() throws Exception {
        // GIVEN
        String id = "id_1";
        String filename = "file_name_1";
        List<String> pathParts = Arrays.asList("path", "to", "file");
        Long size = 125L;
        Long createdTime = 123456L;
        Long modifiedTime = 123456L + 1L;

        // WHEN
        // THEN
        testOutputAsAttributes(
            id,
            filename,
            pathParts,
            size,
            createdTime,
            modifiedTime,
            modifiedTime,
            "/path/to/file"
        );
    }

    @Test
    void testOutputAsContent() throws Exception {
        // GIVEN
        String id = "id_1";
        String filename = "file_name_1";
        List<String> pathParts = Arrays.asList("path", "to", "file");
        Long size = 125L;
        Long createdTime = 123456L;
        Long modifiedTime = 123456L + 1L;

        addJsonRecordWriterFactory();

        mockFetchedFileList(id, filename, pathParts, size, createdTime, modifiedTime);

        List<String> expectedContents = Arrays.asList(
            "[" +
                "{" +
                "\"box.id\":\"" + id + "\"," +
                "\"filename\":\"" + filename + "\"," +
                "\"path\":\"/path/to/file\"," +
                "\"box.size\":" + size + "," +
                "\"box.timestamp\":" + modifiedTime +
                "}" +
                "]");

        // WHEN
        testRunner.run();

        // THEN
        testRunner.assertContents(ListBoxFile.REL_SUCCESS, expectedContents);
    }

    private void addJsonRecordWriterFactory() throws InitializationException {
        RecordSetWriterFactory recordSetWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record_writer", recordSetWriter);
        testRunner.enableControllerService(recordSetWriter);
        testRunner.setProperty(ListBoxFile.RECORD_WRITER, "record_writer");
    }

    private void testOutputAsAttributes(
        String id,
        String filename,
        Collection<String> pathParts,
        Long size,
        Long createdTime,
        Long modifiedTime,
        Long expectedTimestamp,
        String expectedPath
    ) {
        // GIVEN
        mockFetchedFileList(id, filename, pathParts, size, createdTime, modifiedTime);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("box.id", id);
        inputFlowFileAttributes.put("filename", filename);
        inputFlowFileAttributes.put("path", expectedPath);
        inputFlowFileAttributes.put("box.size", "" + size);
        inputFlowFileAttributes.put("box.timestamp", "" + expectedTimestamp);

        HashSet<Map<String, String>> expectedAttributes = new HashSet<>(Arrays.asList(inputFlowFileAttributes));

        // WHEN
        testRunner.run();

        // THEN
        testRunner.assertAttributes(ListBoxFile.REL_SUCCESS, getCheckedAttributeNames(), expectedAttributes);
    }

    private Set<String> getCheckedAttributeNames() {
        Set<String> checkedAttributeNames = Arrays.stream(BoxFlowFileAttribute.values())
            .map(BoxFlowFileAttribute::getName)
            .collect(Collectors.toSet());

        return checkedAttributeNames;
    }

    @Override
    public BoxFolder getMockBoxFolder() {
        return mockBoxFolder;
    }
}
