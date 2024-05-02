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

package org.apache.nifi.minifi.c2;

import static org.apache.nifi.minifi.c2.FileBasedOperationQueueDAO.REQUESTED_OPERATIONS_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.nifi.c2.client.service.operation.OperationQueue;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FileBasedOperationQueueDAOTest {

    @Mock
    private ObjectMapper objectMapper;

    @TempDir
    File tmpDir;

    private FileBasedOperationQueueDAO fileBasedRequestedOperationDAO;

    @BeforeEach
    void setup() {
        fileBasedRequestedOperationDAO = new FileBasedOperationQueueDAO(tmpDir.getAbsolutePath(), objectMapper);
    }

    @Test
    void shouldSaveRequestedOperationsToFile() throws IOException {
        OperationQueue operationQueue = getOperationQueue();
        fileBasedRequestedOperationDAO.save(operationQueue);

        verify(objectMapper).writeValue(any(File.class), eq(operationQueue));
    }

    @Test
    void shouldThrowRuntimeExceptionWhenExceptionHappensDuringSave() throws IOException {
        doThrow(new RuntimeException()).when(objectMapper).writeValue(any(File.class), anyList());

        assertThrows(RuntimeException.class, () -> fileBasedRequestedOperationDAO.save(mock(OperationQueue.class)));
    }

    @Test
    void shouldGetReturnEmptyWhenFileDoesntExists() {
        assertEquals(Optional.empty(), fileBasedRequestedOperationDAO.load());
    }

    @Test
    void shouldGetReturnEmptyWhenExceptionHappens() throws IOException {
        new File(tmpDir.getAbsolutePath() + "/" + REQUESTED_OPERATIONS_FILE_NAME).createNewFile();

        doThrow(new RuntimeException()).when(objectMapper).readValue(any(File.class), eq(OperationQueue.class));

        assertEquals(Optional.empty(), fileBasedRequestedOperationDAO.load());
    }

    @Test
    void shouldGetRequestedOperations() throws IOException {
        new File(tmpDir.getAbsolutePath() + "/" + REQUESTED_OPERATIONS_FILE_NAME).createNewFile();

        OperationQueue operationQueue = getOperationQueue();
        when(objectMapper.readValue(any(File.class), eq(OperationQueue.class))).thenReturn(operationQueue);

        assertEquals(Optional.of(operationQueue), fileBasedRequestedOperationDAO.load());
    }

    private OperationQueue getOperationQueue() {
        C2Operation c2Operation = new C2Operation();
        c2Operation.setIdentifier("id");
        c2Operation.setOperation(OperationType.TRANSFER);
        c2Operation.setOperand(OperandType.DEBUG);
        c2Operation.setArgs(Collections.singletonMap("key", "value"));

        C2Operation currentOperation = new C2Operation();
        currentOperation.setIdentifier("id2");

        return new OperationQueue(currentOperation, List.of(c2Operation));
    }
}