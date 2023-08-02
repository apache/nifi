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
package org.apache.nifi.processors.transfer;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ResourceTransferUtilsTest {

    private static final long FLOW_FILE_SIZE = 2;
    private static final long FILE_RESOURCE_SIZE = 5;

    @Mock
    private InputStream flowFileInputstream;

    @Mock
    private InputStream fileRessourceInputstream;

    @Mock
    private ProcessContext context;

    @Mock(strictness = LENIENT)
    private ProcessSession session;

    @Mock(strictness = LENIENT)
    private FlowFile flowFile;

    private FileResource fileResource;

    @BeforeEach
    void setUp() {
        when(session.read(flowFile)).thenReturn(flowFileInputstream);

        when(flowFile.getSize()).thenReturn(FLOW_FILE_SIZE);

        fileResource = new FileResource(fileRessourceInputstream, FILE_RESOURCE_SIZE);
    }

    @Test
    void testGetFileResourceWhenDataUploadSourceIsLocalFile() {
        final FileResourceService service = mock(FileResourceService.class);
        when(service.getFileResource(any())).thenReturn(fileResource);

        final PropertyValue property = mock(PropertyValue.class);
        when(property.isSet()).thenReturn(true);
        when(property.asControllerService(FileResourceService.class)).thenReturn(service);

        when(context.getProperty(ResourceTransferProperties.FILE_RESOURCE_SERVICE)).thenReturn(property);

        final Optional<FileResource> fileResourceFound = ResourceTransferUtils.getFileResource(ResourceTransferSource.FILE_RESOURCE_SERVICE, context, Collections.emptyMap());

        assertFalse(fileResourceFound.isEmpty());
        assertSame(fileResource, fileResourceFound.get());
    }

    @Test
    void testGetFileResourceWhenDataUploadSourceIsLocalFileButNoServiceConfigured() {
        assertThrows(ProcessException.class, () -> ResourceTransferUtils.getFileResource(ResourceTransferSource.FILE_RESOURCE_SERVICE, context, Collections.emptyMap()));
    }

    @Test
    void testGetFileResourceWhenDataUploadSourceIsFlowFileContent() {
        final Optional<FileResource> fileResourceFound = ResourceTransferUtils.getFileResource(ResourceTransferSource.FLOWFILE_CONTENT, context, Collections.emptyMap());

        assertTrue(fileResourceFound.isEmpty());
    }
}
