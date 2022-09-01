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
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.EqualsWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.apache.nifi.util.EqualsWrapper.wrapList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ListBoxFileSimpleTest implements SimpleListBoxFileTestTrait {
    private ListBoxFile testSubject;

    private ProcessContext mockProcessContext;
    private BoxClientService mockBoxClientService;
    private PropertyValue mockBoxClientServicePropertyValue;
    private BoxAPIConnection mockBoxAPIConnection;

    private BoxFolder mockBoxFolder;

    @BeforeEach
    void setUp() throws Exception {
        mockProcessContext = mock(ProcessContext.class, Answers.RETURNS_DEEP_STUBS);
        mockBoxClientService = mock(BoxClientService.class);
        mockBoxClientServicePropertyValue = mock(PropertyValue.class);
        mockBoxAPIConnection = mock(BoxAPIConnection.class);

        mockBoxFolder = mock(BoxFolder.class);

        testSubject = new ListBoxFile() {
            @Override
            BoxFolder getFolder(String folderId) {
                return mockBoxFolder;
            }
        };

        doReturn(mockBoxClientServicePropertyValue).when(mockProcessContext).getProperty(BoxClientService.BOX_CLIENT_SERVICE);
        doReturn(mockBoxClientService).when(mockBoxClientServicePropertyValue).asControllerService(BoxClientService.class);
        doReturn(mockBoxAPIConnection).when(mockBoxClientService).getBoxApiConnection();

        testSubject.onScheduled(mockProcessContext);
    }

    @Test
    void testCreatedListableEntityContainsCorrectData() throws Exception {
        // GIVEN
        Long minTimestamp = 0L;

        String id = "id_1";
        String filename = "file_name_1";
        List<String> pathParts = Arrays.asList("path", "to", "file");
        Long size = 125L;
        long createdTime = 123456L;
        long modifiedTime = 234567L;

        mockFetchedFileList(id, filename, pathParts, size, createdTime, modifiedTime);

        List<BoxFileInfo> expected = Arrays.asList(
            new BoxFileInfo.Builder()
                .id(id)
                .fileName(filename)
                .path("/path/to/file")
                .size(size)
                .createdTime(createdTime)
                .modifiedTime(modifiedTime)
                .build()
        );

        // WHEN
        List<BoxFileInfo> actual = testSubject.performListing(mockProcessContext, minTimestamp, null);

        // THEN
        List<Function<BoxFileInfo, Object>> propertyProviders = Arrays.asList(
            BoxFileInfo::getId,
            BoxFileInfo::getIdentifier,
            BoxFileInfo::getName,
            BoxFileInfo::getPath,
            BoxFileInfo::getSize,
            BoxFileInfo::getTimestamp,
            BoxFileInfo::getCreatedTime,
            BoxFileInfo::getModifiedTime
        );

        List<EqualsWrapper<BoxFileInfo>> expectedWrapper = wrapList(expected, propertyProviders);
        List<EqualsWrapper<BoxFileInfo>> actualWrapper = wrapList(actual, propertyProviders);

        assertEquals(expectedWrapper, actualWrapper);
    }

    @Override
    public BoxFolder getMockBoxFolder() {
        return mockBoxFolder;
    }
}
