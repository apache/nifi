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
package org.apache.nifi.processors.dataupload;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class DataUploadUtilTest {

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

        when(context.getProperty(DataUploadProperties.FILE_RESOURCE_SERVICE)).thenReturn(property);

        final FileResource result = DataUploadUtil.getFileResource(DataUploadSource.LOCAL_FILE, context, flowFile);

        assertSame(fileResource, result);
    }

    @Test
    void testGetFileResourceWhenDataUploadSourceIsLocalFileButNoServiceConfigured() {
        assertThrows(ProcessException.class, () -> DataUploadUtil.getFileResource(DataUploadSource.LOCAL_FILE, context, flowFile));
    }

    @Test
    void testGetFileResourceWhenDataUploadSourceIsFlowFileContent() {
        final FileResource result = DataUploadUtil.getFileResource(DataUploadSource.FLOWFILE_CONTENT, context, flowFile);

        assertNull(result);
    }

    @Test
    void testGetUploadInputStreamWithFlowFile() {
        final InputStream inputStream = DataUploadUtil.getUploadInputStream(session, flowFile, null);

        assertSame(flowFileInputstream, inputStream);
    }

    @Test
    void testGetUploadInputStreamWithFileResource() {
        final InputStream inputStream = DataUploadUtil.getUploadInputStream(session, flowFile, fileResource);

        assertSame(fileRessourceInputstream, inputStream);
    }

    @Test
    void testGetUploadSizeWithFlowFile() {
        final long size = DataUploadUtil.getUploadSize(flowFile, null);

        assertEquals(FLOW_FILE_SIZE, size);
    }

    @Test
    void testGetUploadSizeWithFileResource() {
        final long size = DataUploadUtil.getUploadSize(flowFile, fileResource);

        assertEquals(FILE_RESOURCE_SIZE, size);
    }
}
