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
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.EqualsWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.nifi.util.EqualsWrapper.wrapList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class ListBoxFileListingTest implements FileListingTestTrait {
    private ListBoxFile testSubject;

    @Mock
    private ProcessContext mockProcessContext;
    @Mock
    private BoxClientService mockBoxClientService;
    @Mock
    private PropertyValue mockBoxClientServicePropertyValue;
    @Mock
    private PropertyValue mockFolderIdPropertyValue;
    @Mock
    private PropertyValue mockRecursiveSearchPropertyValue;
    @Mock
    private PropertyValue mockMinAgePropertyValue;
    @Mock
    private BoxClient mockBoxClient;

    @BeforeEach
    void setUp() {
        testSubject = new ListBoxFile();

        // Mock BOX_CLIENT_SERVICE property
        doReturn(mockBoxClientServicePropertyValue).when(mockProcessContext).getProperty(AbstractBoxProcessor.BOX_CLIENT_SERVICE);
        doReturn(mockBoxClientService).when(mockBoxClientServicePropertyValue).asControllerService(BoxClientService.class);
        doReturn(mockBoxClient).when(mockBoxClientService).getBoxClient();

        // Mock FOLDER_ID property with proper chain
        doReturn(mockFolderIdPropertyValue).when(mockProcessContext).getProperty(ListBoxFile.FOLDER_ID);
        PropertyValue evaluatedFolderIdPropertyValue = mock(PropertyValue.class);
        doReturn(evaluatedFolderIdPropertyValue).when(mockFolderIdPropertyValue).evaluateAttributeExpressions();
        doReturn("testFolderId").when(evaluatedFolderIdPropertyValue).getValue();

        // Mock RECURSIVE_SEARCH property
        doReturn(mockRecursiveSearchPropertyValue).when(mockProcessContext).getProperty(ListBoxFile.RECURSIVE_SEARCH);
        doReturn(false).when(mockRecursiveSearchPropertyValue).asBoolean();

        // Mock MIN_AGE property
        doReturn(mockMinAgePropertyValue).when(mockProcessContext).getProperty(ListBoxFile.MIN_AGE);
        doReturn(0L).when(mockMinAgePropertyValue).asTimePeriod(TimeUnit.MILLISECONDS);

        testSubject.onScheduled(mockProcessContext);
    }

    @Test
    void testCreatedListableEntityContainsCorrectData() {

        Long minTimestamp = 0L;

        String id = "id_1";
        String filename = "file_name_1";
        List<String> pathParts = Arrays.asList("path", "to", "file");
        long size = 125L;
        long createdTime = 123456L;
        long modifiedTime = 234567L;

        mockFetchedFileList(id, filename, pathParts, size, createdTime, modifiedTime);

        List<BoxFileInfo> expected = singletonList(
            new BoxFileInfo.Builder()
                .id(id)
                .fileName(filename)
                .path("/path/to/file")
                .size(size)
                .createdTime(createdTime)
                .modifiedTime(modifiedTime)
                .build()
        );

        List<BoxFileInfo> actual = testSubject.performListing(mockProcessContext, minTimestamp, null);

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
    public BoxClient getMockBoxClient() {
        return mockBoxClient;
    }
}
