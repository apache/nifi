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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.util.EqualsWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.apache.nifi.util.EqualsWrapper.wrapList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

public class ListBoxFilesSimpleTest implements SimpleListBoxFileTestTrait {
    private ListBoxFiles testSubject;

    private ProcessContext mockProcessContext;
    private BoxAPIConnection mockBoxAPIConnection;

    private BoxFolder mockBoxFolder;

    @BeforeEach
    void setUp() throws Exception {
        mockProcessContext = mock(ProcessContext.class, RETURNS_DEEP_STUBS);
        mockBoxAPIConnection = mock(BoxAPIConnection.class, RETURNS_DEEP_STUBS);
        mockBoxFolder = mock(BoxFolder.class, RETURNS_DEEP_STUBS);

        testSubject = new ListBoxFiles() {
            @Override
            protected List<BoxFileInfo> performListing(ProcessContext context, Long minTimestamp, AbstractListProcessor.ListingMode ignoredListingMode) throws IOException {
                return super.performListing(context, minTimestamp, ListingMode.EXECUTION);
            }

            @Override
            public BoxAPIConnection createBoxApiConnection(ProcessContext context, ProxyConfiguration proxyConfiguration) {
                return mockBoxAPIConnection;
            }

            @Override
            BoxFolder getFolder(String folderId) {
                return mockBoxFolder;
            }
        };

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
