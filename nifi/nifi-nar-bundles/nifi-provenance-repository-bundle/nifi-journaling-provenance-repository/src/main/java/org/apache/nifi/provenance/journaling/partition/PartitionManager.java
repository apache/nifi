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
     * Performs the given Action on each partition and returns the set of results. This method does 
     * not use the thread pool in order to perform the request in parallel. This is desirable for 
     * very quick functions, as the thread pool can be fully utilized, resulting in a quick function 
     * taking far longer than it should.
     * 
     * @param action the action to perform
     * @param writeAction specifies whether or not the action writes to the repository
     * @return
     */
    <T> Set<T> withEachPartitionSerially(PartitionAction<T> action, boolean writeAction) throws IOException;
    
    
    /**
     * Performs the given Action on each partition. Unlike
     * {@link #withEachPartition(PartitionAction))}, this method does not use the thread pool
     * in order to perform the request in parallel. This is desirable for very quick functions,
     * as the thread pool can be fully utilized, resulting in a quick function taking far longer
     * than it should.
     * 
     * @param action the action to perform
     * @param writeAction specifies whether or not the action writes to the repository
     * @return
     */
    void withEachPartitionSerially(VoidPartitionAction action, boolean writeAction) throws IOException;
    
    void shutdown();
    
    /**
     * Triggers the Partition Manager to delete events from journals and indices based on the sizes of the containers
     * and overall size of the repository
     */
    void deleteEventsBasedOnSize();
}
