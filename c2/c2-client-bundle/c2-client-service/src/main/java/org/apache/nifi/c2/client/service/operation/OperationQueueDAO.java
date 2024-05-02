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

package org.apache.nifi.c2.client.service.operation;

import java.util.Optional;

/**
 * The purpose of this interface is to be able to persist operations between restarts.
 */
public interface OperationQueueDAO {

    /**
     * Persist the given requested operation list
     * @param operationQueue the queue containing the current and remaining operations
     */
    void save(OperationQueue operationQueue);

    /**
     * Returns the saved Operations
     *
     * @return the C2 Operations queue with the actual operation
     */
    Optional<OperationQueue> load();

    /**
     * Resets the saved operations
     */
    void cleanup();

}
