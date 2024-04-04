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
package org.apache.nifi.processors.azure.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;

import static org.apache.nifi.processors.azure.storage.PutAzureDataLakeStorage.FAIL_RESOLUTION;
import static org.apache.nifi.processors.azure.storage.PutAzureDataLakeStorage.IGNORE_RESOLUTION;
import static org.apache.nifi.processors.azure.storage.PutAzureDataLakeStorage.REPLACE_RESOLUTION;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPutAzureDataLakeStorage {

    private static final String FILE_NAME = "file1";

    @Test
    public void testPutFileButFailedToRenameWithUnrecoverableError() {
        final PutAzureDataLakeStorage processor = new PutAzureDataLakeStorage();
        final ProcessorInitializationContext initContext = mock(ProcessorInitializationContext.class);
        final String componentId = "componentId";
        final DataLakeFileClient fileClient = mock(DataLakeFileClient.class);
        final Response<DataLakeFileClient> response = mock(Response.class);
        final DataLakeStorageException exception = mock(DataLakeStorageException.class);
        //Mock logger
        when(initContext.getIdentifier()).thenReturn(componentId);
        MockComponentLog componentLog = new MockComponentLog(componentId, processor);
        when(initContext.getLogger()).thenReturn(componentLog);
        processor.initialize(initContext);
        //Mock renameWithResponse Azure method
        when(fileClient.renameWithResponse(isNull(), anyString(), isNull(), any(DataLakeRequestConditions.class), isNull(), isNull())).thenReturn(response);
        when(fileClient.getFileName()).thenReturn(FILE_NAME);
        when(exception.getStatusCode()).thenReturn(405);
        when(response.getValue()).thenThrow(exception);
        assertThrows(ProcessException.class, () -> processor.renameFile(fileClient, "", FILE_NAME, FAIL_RESOLUTION));
        assertThrows(ProcessException.class, () -> processor.renameFile(fileClient, "", FILE_NAME, REPLACE_RESOLUTION));
        assertThrows(ProcessException.class, () -> processor.renameFile(fileClient, "", FILE_NAME, IGNORE_RESOLUTION));
        verify(fileClient, times(3)).delete();
    }
}