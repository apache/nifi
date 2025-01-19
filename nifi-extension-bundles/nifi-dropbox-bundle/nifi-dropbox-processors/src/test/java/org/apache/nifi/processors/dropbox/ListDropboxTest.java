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

import static java.util.Collections.singletonList;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DbxUserFilesRequests;
import com.dropbox.core.v2.files.DbxUserListFolderBuilder;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.StreamSupport;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ListDropboxTest extends AbstractDropboxTest {

    public static final String FOLDER_ID = "id:11111";
    public static final boolean IS_RECURSIVE = true;
    public static final long MIN_TIMESTAMP = 1659707000;
    public static final long OLD_CREATED_TIME = 1657375066;

    @Mock
    private DbxClientV2 mockDropboxClient;

    @Mock
    private DbxUserFilesRequests mockDbxUserFilesRequest;

    @Mock
    private ListFolderResult mockListFolderResult;

    @Mock
    private DbxUserListFolderBuilder mockListFolderBuilder;

    @BeforeEach
    protected void setUp() throws Exception {
        ListDropbox testSubject = new ListDropbox() {
            @Override
            public DbxClientV2 getDropboxApiClient(ProcessContext context, String id) {
                return mockDropboxClient;
            }

            @Override
            protected List<DropboxFileInfo> performListing(
                    ProcessContext context, Long minTimestamp, ListingMode ignoredListingMode) throws IOException {
                return super.performListing(context, MIN_TIMESTAMP, ListingMode.EXECUTION);
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);

        testRunner.setProperty(ListDropbox.RECURSIVE_SEARCH, Boolean.toString(IS_RECURSIVE));
        testRunner.setProperty(ListDropbox.MIN_AGE, "0 sec");
        super.setUp();
    }

    @Test
    void testFolderValidity() {
        testRunner.setProperty(ListDropbox.FOLDER, "id:odTlUvbpIEAAAAAAAAABmw");
        testRunner.assertValid();
        testRunner.setProperty(ListDropbox.FOLDER, "/");
        testRunner.assertValid();
        testRunner.setProperty(ListDropbox.FOLDER, "/tempFolder");
        testRunner.assertValid();
        testRunner.setProperty(ListDropbox.FOLDER, "/tempFolder/tempSubFolder");
        testRunner.assertValid();
        testRunner.setProperty(ListDropbox.FOLDER, "/tempFolder/tempSubFolder/");
        testRunner.assertValid();
        testRunner.setProperty(ListDropbox.FOLDER, "tempFolder");
        testRunner.assertNotValid();
        testRunner.setProperty(ListDropbox.FOLDER, "odTlUvbpIEAAAAAAAAABmw");
        testRunner.assertNotValid();
        testRunner.setProperty(ListDropbox.FOLDER, "");
        testRunner.assertNotValid();
    }

    @Test
    void testRootIsListed() throws Exception {
        mockFileListing();

        String folderName = "/";
        testRunner.setProperty(ListDropbox.FOLDER, folderName);

        //root is listed when "" is used in Dropbox API
        when(mockDbxUserFilesRequest.listFolderBuilder("")).thenReturn(mockListFolderBuilder);
        when(mockListFolderResult.getEntries()).thenReturn(singletonList(
                createFileMetadata(FILE_ID_1, FILENAME_1, folderName, CREATED_TIME)
        ));

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ListDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.getFirst();
        assertOutFlowFileAttributes(ff0, folderName);
    }

    @Test
    void testOnlyFilesAreListedFoldersAndShortcutsAreFiltered() throws Exception {
        mockFileListing();

        testRunner.setProperty(ListDropbox.FOLDER, TEST_FOLDER);

        when(mockDbxUserFilesRequest.listFolderBuilder(TEST_FOLDER)).thenReturn(mockListFolderBuilder);
        when(mockListFolderResult.getEntries()).thenReturn(Arrays.asList(
                createFileMetadata(FILE_ID_1, FILENAME_1, TEST_FOLDER, CREATED_TIME),
                createFolderMetadata(),
                createFileMetadata(FILE_ID_2, FILENAME_2, TEST_FOLDER, CREATED_TIME, false)
        ));

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ListDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.getFirst();
        assertOutFlowFileAttributes(ff0);
    }

    @Test
    void testOldItemIsFiltered() throws Exception {
        mockFileListing();

        testRunner.setProperty(ListDropbox.FOLDER, TEST_FOLDER);

        when(mockDbxUserFilesRequest.listFolderBuilder(TEST_FOLDER)).thenReturn(mockListFolderBuilder);
        when(mockListFolderResult.getEntries()).thenReturn(Arrays.asList(
                createFileMetadata(FILE_ID_1, FILENAME_1, TEST_FOLDER, CREATED_TIME),
                createFileMetadata(FILE_ID_2, FILENAME_2, TEST_FOLDER, OLD_CREATED_TIME)
        ));

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ListDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.getFirst();
        assertOutFlowFileAttributes(ff0);
    }

    @Test
    void testRecordWriter() throws Exception {
        mockFileListing();
        mockRecordWriter();

        testRunner.setProperty(ListDropbox.FOLDER, TEST_FOLDER);

        when(mockDbxUserFilesRequest.listFolderBuilder(TEST_FOLDER)).thenReturn(mockListFolderBuilder);
        when(mockListFolderResult.getEntries()).thenReturn(Arrays.asList(
                createFileMetadata(FILE_ID_1, FILENAME_1, TEST_FOLDER, CREATED_TIME),
                createFileMetadata(FILE_ID_2, FILENAME_2, TEST_FOLDER, CREATED_TIME)
        ));

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ListDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.getFirst();
        List<String> expectedFileNames = Arrays.asList(FILENAME_1, FILENAME_2);
        List<String> actualFileNames = getFilenames(ff0.getContent());

        assertEquals(expectedFileNames, actualFileNames);
    }

    private Metadata createFolderMetadata() {
        return FolderMetadata.newBuilder(FOLDER_ID)
                .withPathDisplay(TEST_FOLDER + "/" + FOLDER_ID)
                .build();
    }

    private void mockRecordWriter() throws InitializationException {
        RecordSetWriterFactory recordWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record_writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(ListDropbox.RECORD_WRITER, "record_writer");
    }

    private void mockFileListing() throws DbxException {
        when(mockListFolderBuilder.withRecursive(IS_RECURSIVE)).thenReturn(mockListFolderBuilder);
        when(mockListFolderBuilder.start()).thenReturn(mockListFolderResult);
        when(mockDropboxClient.files()).thenReturn(mockDbxUserFilesRequest);
        when(mockListFolderResult.getHasMore()).thenReturn(false);
    }

    private List<String> getFilenames(String flowFileContent) {
        try {
            JsonNode jsonNode = new ObjectMapper().readTree(flowFileContent);
            return StreamSupport.stream(spliteratorUnknownSize(jsonNode.iterator(), Spliterator.ORDERED), false)
                    .map(node -> node.get("filename").asText())
                    .collect(toList());
        } catch (JsonProcessingException e) {
            return Collections.emptyList();
        }
    }
}
