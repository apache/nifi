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
package org.apache.nifi.provenance.journaling.partition;

import java.io.IOException;
import java.util.Set;


/**
 * The PartitionManager is responsible for accessing and maintaining the Partitions so that they are
 * are written to efficiently and in a thread-safe manner.
 */
public interface PartitionManager {

    /**
     * Performs the given action against one of the partitions
     * 
     * @param action the action to perform
     * @param writeAction specifies whether or not the action writes to the repository
     * @return
     * @throws IOException
     */
    <T> T withPartition(PartitionAction<T> action, boolean writeAction) throws IOException;
    
    /**
     * Performs the given action against one of the partitions
     * 
     * @param action the action to perform
     * @param writeAction specifies whether or not the action writes to the repository
     * @throws IOException
     */
    void withPartition(VoidPartitionAction action, boolean writeAction) throws IOException;
    
    /**
     * Performs the given Action on each partition and returns the set of results.
     * 
     * @param action the action to perform
     * @param writeAction specifies whether or not the action writes to the repository
     * @return
     */
    <T> Set<T> withEachPartition(PartitionAction<T> action) throws IOException;
    
    /**
     * Performs the given Action to each partition, optionally waiting for the action to complete
     * @param action
     * @param writeAction
     * @param async if <code>true</code>, will perform the action asynchronously; if <code>false</code>, will
     *  wait for the action to complete before returning
     */
    void withEachPartition(VoidPartitionAction action, boolean async);
    
    void shutdown();
}
