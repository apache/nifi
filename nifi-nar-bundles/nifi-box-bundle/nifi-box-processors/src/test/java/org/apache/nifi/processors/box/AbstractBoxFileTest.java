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

import static java.lang.String.valueOf;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxFolder.Info;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AbstractBoxFileTest {
    public final String TEST_FILE_ID = "fileId";
    public final String TEST_FOLDER_ID = "folderId";
    public final String TEST_FILENAME = "filename";
    public final String TEST_FOLDER_NAME = "folderName";
    public final long TEST_SIZE = 12L;
    public final long CREATED_TIME = 123456L;
    public final long MODIFIED_TIME = 7891011L;
    public final String CONTENT = "content";

    protected TestRunner testRunner;

    @Mock
    protected BoxFolder mockBoxFolder;

    @Mock
    protected BoxClientService mockBoxClientService;

    @Mock
    protected BoxAPIConnection mockBoxAPIConnection;

    @Mock
    protected BoxFile.Info mockFileInfo;

    @Mock
    protected BoxFolder.Info mockBoxFolderInfo;


    @BeforeEach
    void setUp() throws Exception {
        doReturn(mockBoxClientService.toString()).when(mockBoxClientService).getIdentifier();
        doReturn(mockBoxAPIConnection).when(mockBoxClientService).getBoxApiConnection();

        testRunner.addControllerService(mockBoxClientService.getIdentifier(), mockBoxClientService);
        testRunner.enableControllerService(mockBoxClientService);
        testRunner.setProperty(BoxClientService.BOX_CLIENT_SERVICE, mockBoxClientService.getIdentifier());
    }

    protected BoxFile.Info createFileInfo(String path, Long createdTime) {
        return createFileInfo(path, createdTime, singletonList(mockBoxFolderInfo));
    }

    protected BoxFile.Info createFileInfo(String path, Long createdTime, List<Info> pathCollection) {
        when(mockBoxFolderInfo.getName()).thenReturn(path);
        when(mockBoxFolderInfo.getID()).thenReturn("not0");

        when(mockFileInfo.getID()).thenReturn(TEST_FILE_ID);
        when(mockFileInfo.getName()).thenReturn(TEST_FILENAME);
        when(mockFileInfo.getPathCollection()).thenReturn(pathCollection);
        when(mockFileInfo.getSize()).thenReturn(TEST_SIZE);
        when(mockFileInfo.getModifiedAt()).thenReturn(new Date(createdTime));

        return mockFileInfo;
    }

    protected void assertProvenanceEvent(ProvenanceEventType eventType) {
        Set<ProvenanceEventType> expectedEventTypes = Collections.singleton(eventType);
        Set<ProvenanceEventType> actualEventTypes = testRunner.getProvenanceEvents().stream()
                .map(ProvenanceEventRecord::getEventType)
                .collect(toSet());
        assertEquals(expectedEventTypes, actualEventTypes);
    }

    protected void assertNoProvenanceEvent() {
        assertTrue(testRunner.getProvenanceEvents().isEmpty());
    }

    protected void assertOutFlowFileAttributes(MockFlowFile flowFile) {
        assertOutFlowFileAttributes(flowFile, "/" + TEST_FOLDER_NAME);
    }

    protected void assertOutFlowFileAttributes(MockFlowFile flowFile, String path) {
        flowFile.assertAttributeEquals(BoxFileAttributes.ID, TEST_FILE_ID);
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), TEST_FILENAME);
        flowFile.assertAttributeEquals(CoreAttributes.PATH.key(), path);
        flowFile.assertAttributeEquals(BoxFileAttributes.TIMESTAMP, valueOf(new Date(MODIFIED_TIME)));
        flowFile.assertAttributeEquals(BoxFileAttributes.SIZE, valueOf(TEST_SIZE));
    }
}
