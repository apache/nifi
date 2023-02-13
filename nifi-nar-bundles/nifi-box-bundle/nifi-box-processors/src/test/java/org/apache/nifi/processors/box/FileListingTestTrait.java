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

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public interface FileListingTestTrait {
    BoxFolder getMockBoxFolder();

    default void mockFetchedFileList(
        String id,
        String filename,
        Collection<String> pathParts,
        Long size,
        Long createdTime,
        Long modifiedTime
    ) {
        doReturn(singletonList(createFileInfo(
                                id,
                                filename,
                                pathParts,
                                size,
                                createdTime,
                                modifiedTime
                        )
                )
        ).when(getMockBoxFolder()).getChildren("id",
            "name",
            "item_status",
            "size",
            "created_at",
            "modified_at",
            "content_created_at",
            "content_modified_at",
            "path_collection");
    }

    default BoxFile.Info createFileInfo(
        String id,
        String name,
        Collection<String> pathParts,
        Long size,
        Long createdTime,
        Long modifiedTime
    ) {
        BoxFile.Info fileInfo = mock(BoxFile.Info.class);

        List<BoxFolder.Info> pathCollection = pathParts.stream().map(pathPart -> {
            BoxFolder.Info folderInfo = mock(BoxFolder.Info.class);
            when(folderInfo.getName()).thenReturn(pathPart);
            when(folderInfo.getID()).thenReturn("not0");

            return folderInfo;
        }).collect(Collectors.toList());

        when(fileInfo.getID()).thenReturn(id);
        when(fileInfo.getName()).thenReturn(name);
        when(fileInfo.getPathCollection()).thenReturn(pathCollection);
        when(fileInfo.getSize()).thenReturn(size);
        when(fileInfo.getCreatedAt()).thenReturn(new Date(createdTime));
        when(fileInfo.getModifiedAt()).thenReturn(new Date(modifiedTime));

        return fileInfo;
    }
}
