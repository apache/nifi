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

import com.box.sdkgen.client.BoxClient;
import com.box.sdkgen.managers.folders.FoldersManager;
import com.box.sdkgen.managers.folders.GetFolderItemsQueryParams;
import com.box.sdkgen.schemas.file.FilePathCollectionField;
import com.box.sdkgen.schemas.filefull.FileFull;
import com.box.sdkgen.schemas.foldermini.FolderMini;
import com.box.sdkgen.schemas.items.Items;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public interface FileListingTestTrait {

    BoxClient getMockBoxClient();

    default FileFull createFileInfo(
        String id,
        String name,
        Collection<String> pathParts,
        Long size,
        Long createdTime,
        Long modifiedTime
    ) {
        FileFull fileInfo = mock(FileFull.class);

        List<FolderMini> pathCollection = pathParts.stream().map(pathPart -> {
            FolderMini folderInfo = mock(FolderMini.class);
            when(folderInfo.getName()).thenReturn(pathPart);
            when(folderInfo.getId()).thenReturn("not0");

            return folderInfo;
        }).collect(Collectors.toList());

        FilePathCollectionField pathCollectionField = mock(FilePathCollectionField.class);
        when(pathCollectionField.getEntries()).thenReturn(pathCollection);

        when(fileInfo.getId()).thenReturn(id);
        when(fileInfo.getName()).thenReturn(name);
        when(fileInfo.getPathCollection()).thenReturn(pathCollectionField);
        when(fileInfo.getSize()).thenReturn(size);
        when(fileInfo.getCreatedAt()).thenReturn(OffsetDateTime.ofInstant(Instant.ofEpochMilli(createdTime), ZoneOffset.UTC));
        when(fileInfo.getModifiedAt()).thenReturn(OffsetDateTime.ofInstant(Instant.ofEpochMilli(modifiedTime), ZoneOffset.UTC));

        return fileInfo;
    }

    @SuppressWarnings("unchecked")
    default void mockFetchedFileList(
        String id,
        String name,
        Collection<String> pathParts,
        Long size,
        Long createdTime,
        Long modifiedTime
    ) {
        FileFull fileInfo = createFileInfo(id, name, pathParts, size, createdTime, modifiedTime);
        Items items = mock(Items.class);

        // Use raw list to avoid generics issues with mockito
        List entries = new ArrayList<>();
        entries.add(fileInfo);
        lenient().when(items.getEntries()).thenReturn(entries);

        FoldersManager foldersManager = mock(FoldersManager.class);
        lenient().when(foldersManager.getFolderItems(anyString(), any(GetFolderItemsQueryParams.class))).thenReturn(items);
        lenient().when(getMockBoxClient().getFolders()).thenReturn(foldersManager);
    }
}
