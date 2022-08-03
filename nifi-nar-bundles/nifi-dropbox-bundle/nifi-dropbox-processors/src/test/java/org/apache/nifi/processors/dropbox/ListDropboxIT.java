/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.dropbox;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ListDropboxIT extends AbstractDropboxIT<ListDropbox>{

    private static final String YOUNG_FILE_NAME = "just_created" ;

    @BeforeEach
    public void init() throws Exception {
        super.init();
        testRunner.setProperty(ListDropbox.FOLDER_NAME, MAIN_FOLDER);
    }

    @Override
    protected ListDropbox createTestSubject() {
        return new ListDropbox();
    }

    @Test
    void testEmbeddedDirectoriesAreListed() throws Exception {
        // GIVEN
        createFile("test_file1", "test_file_content1", MAIN_FOLDER);
        createFile("test_file2", "test_file_content2", MAIN_FOLDER);
        createFile("test_file11", "test_file_content11", MAIN_FOLDER + "/testFolder1");
        createFile("test_file112", "test_file_content112", MAIN_FOLDER + "/testFolder2");

        createFile("test_file_not_in_main_folder", "test_file_content31", "/notMainFolder");

        Set<String> expectedFileNames = new HashSet<>(Arrays.asList("test_file1", "test_file2", "test_file11",
                "test_file112"));

        // The creation of the files are not (completely) synchronized.
        Thread.sleep(2000);

        // WHEN
        testRunner.run();

        // THEN
        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);

        Set<String> actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toSet());

        assertEquals(expectedFileNames, actualFileNames);
    }

    @Test
    void testTooYoungFilesNotListedWhenMinAgeIsSet() throws Exception {
        // GIVEN
        testRunner.setProperty(ListDropbox.MIN_AGE, "15 s");

        createFile(YOUNG_FILE_NAME, "test_file_content1", MAIN_FOLDER);

        // Make sure the file 'arrives' and could be listed
        Thread.sleep(5000);

        // WHEN
        testRunner.run();

        // THEN
        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);

        List<String> actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toList());

        assertEquals(Collections.emptyList(), actualFileNames);

        // Next, wait for another 10+ seconds for MIN_AGE to expire then list again

        // GIVEN
        Thread.sleep(10000);

        List<String> expectedFileNames = Collections.singletonList(YOUNG_FILE_NAME);

        // WHEN
        testRunner.run();

        // THEN
        successFlowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);

        actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toList());

        assertEquals(expectedFileNames, actualFileNames);
    }

    @Test
    void testRecordOutput() throws Exception {
        // GIVEN
        createFile("test_file1", "test_file_content1", MAIN_FOLDER);
        createFile("test_file2", "test_file_content2", MAIN_FOLDER);

        List<String> expectedFileNames = Arrays.asList("test_file1", "test_file2");

        // The creation of the files are not (completely) synchronized.
        Thread.sleep(2000);

        // WHEN
        addJsonRecordSetWriterFactory();
        testRunner.run();

        // THEN

        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);
        assertEquals(1, successFlowFiles.size());

        List<String> actualFileNames = successFlowFiles.stream()
                .map(MockFlowFile::getContent)
                .map(this::getFilenames)
                .flatMap(Collection::stream)
                .collect(toList());

        assertEquals(expectedFileNames, actualFileNames);
    }

    private List<String> getFilenames(String flowFileContent) {
        try {
            JsonNode jsonNode = new ObjectMapper().readTree(flowFileContent);
            return StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(jsonNode.iterator(), Spliterator.ORDERED), false)
                    .map(node -> node.get("filename").asText())
                    .collect(toList());
        } catch (JsonProcessingException e) {
            return Collections.emptyList();
        }
    }

    private void addJsonRecordSetWriterFactory() throws InitializationException {
        RecordSetWriterFactory recordWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record_writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(ListDropbox.RECORD_WRITER, "record_writer");
    }
}
