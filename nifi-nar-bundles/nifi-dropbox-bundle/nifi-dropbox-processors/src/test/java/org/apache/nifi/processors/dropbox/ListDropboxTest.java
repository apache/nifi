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

import static org.apache.nifi.util.EqualsWrapper.wrapList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DbxUserFilesRequests;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderBuilder;
import com.dropbox.core.v2.files.ListFolderResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.EqualsWrapper;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ListDropboxTest {

    private ListDropbox testSubject;

    @Mock
    private ProcessContext mockProcessContext;
    @Mock
    private DbxClientV2 mockDbxClient;
    @Mock
    private DbxUserFilesRequests mockDbxUserFilesRequest;
    @Mock
    private ListFolderResult mockListFolderResult;
    @Mock
    private ListFolderBuilder mockListFolderBuilder;


    @BeforeEach
    void setUp() {
        testSubject = new ListDropbox() {

            @Override
            public DbxClientV2 getDropboxApiClient(ProcessContext context) {
                return mockDbxClient;
            }
        };
        testSubject.onScheduled(mockProcessContext);
    }

    @Test
    void testCreatedListableEntityContainsCorrectDataOldItemFiltered() throws Exception {
        long minTimestamp = 1659707000;

        String id1 = "id:11111";
        String id2 = "id:22222";
        String filename1 = "file_name_1";
        String old_file_name = "old_file_name";
        long size = 125;
        long oldCreatedTime = 1657375066;
        long createdTime = 1659707000;
        String revision = "5e4ddb1320676a5c29261";

        boolean isRecursive = true;
        String folderName = "test_folder";

        when(mockProcessContext.getProperty(ListDropbox.FOLDER_NAME)).thenReturn(new MockPropertyValue(folderName));
        when(mockProcessContext.getProperty(ListDropbox.RECURSIVE_SEARCH))
                .thenReturn(new MockPropertyValue(String.valueOf(isRecursive)));
        when(mockProcessContext.getProperty(ListDropbox.MIN_AGE)).thenReturn(new MockPropertyValue("0 sec"));

        when(mockDbxClient.files()).thenReturn(mockDbxUserFilesRequest);
        when(mockDbxUserFilesRequest.listFolderBuilder(folderName)).thenReturn(mockListFolderBuilder);
        when(mockListFolderBuilder.withRecursive(isRecursive)).thenReturn(mockListFolderBuilder);
        when(mockListFolderBuilder.start()).thenReturn(mockListFolderResult);

        when(mockListFolderResult.getEntries()).thenReturn(Arrays.asList(
                createFileMetaData(filename1, id1, createdTime, revision, size),
                createFileMetaData(old_file_name, id2, oldCreatedTime, revision, size)
        ));
        when(mockListFolderResult.getHasMore()).thenReturn(false);

        List<DropboxFileInfo> expected = Collections.singletonList(
                new DropboxFileInfo.Builder()
                        .id(id1)
                        .name(filename1)
                        .size(size)
                        .timestamp(createdTime)
                        .revision(revision)
                        .build()
        );

        List<DropboxFileInfo> actual = testSubject.performListing(mockProcessContext, minTimestamp, null);

        List<Function<DropboxFileInfo, Object>> propertyProviders = Arrays.asList(
                DropboxFileInfo::getId,
                DropboxFileInfo::getName,
                DropboxFileInfo::getSize,
                DropboxFileInfo::getTimestamp,
                DropboxFileInfo::getRevision
        );

        List<EqualsWrapper<DropboxFileInfo>> expectedWrapper = wrapList(expected, propertyProviders);
        List<EqualsWrapper<DropboxFileInfo>> actualWrapper = wrapList(actual, propertyProviders);

        assertEquals(expectedWrapper, actualWrapper);
    }

    private FileMetadata createFileMetaData(
            String name,
            String id,
            long createdTime,
            String revision,
            Long size) {
        return new FileMetadata(name, id,
                new Date(createdTime),
                new Date(createdTime),
                revision, size);
    }
}
