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

import static org.slf4j.LoggerFactory.getLogger;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.Optional;
import org.apache.nifi.c2.client.service.operation.OperationQueue;
import org.apache.nifi.c2.client.service.operation.OperationQueueDAO;
import org.slf4j.Logger;

public class FileBasedOperationQueueDAO implements OperationQueueDAO {

    private static final Logger LOGGER = getLogger(FileBasedOperationQueueDAO.class);

    protected static final String REQUESTED_OPERATIONS_FILE_NAME = "requestedOperations.data";

    private final ObjectMapper objectMapper;
    private final File requestedOperationsFile;

    public FileBasedOperationQueueDAO(String runDir, ObjectMapper objectMapper) {
        this.requestedOperationsFile = new File(runDir, REQUESTED_OPERATIONS_FILE_NAME);
        this.objectMapper = objectMapper;
    }

    public void save(OperationQueue operationQueue) {
        LOGGER.info("Saving C2 operations to file");
        LOGGER.debug("C2 Operation Queue: {}", operationQueue);
        try {
            objectMapper.writeValue(requestedOperationsFile, operationQueue);
        } catch (Exception e) {
            LOGGER.error("Failed to save requested c2 operations", e);
            throw new RuntimeException(e);
        }
    }

    public Optional<OperationQueue> load() {
        LOGGER.info("Reading queued c2 operations from file");
        if (requestedOperationsFile.exists()) {
            try {
                OperationQueue operationQueue = objectMapper.readValue(requestedOperationsFile, OperationQueue.class);
                LOGGER.debug("Queued operations: {}", operationQueue);
                return Optional.of(operationQueue);
            } catch (Exception e) {
                LOGGER.error("Failed to read queued operations file", e);
            }
        } else {
            LOGGER.info("There is no queued c2 operation");
        }
        return Optional.empty();
    }

    public void cleanup() {
        if (requestedOperationsFile.exists() && !requestedOperationsFile.delete()) {
            LOGGER.error("Failed to delete requested operations file {}, it should be deleted manually", requestedOperationsFile);
        }
    }

}
