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
package org.apache.nifi.controller.repository;

import java.io.IOException;
import java.util.List;

import org.apache.nifi.controller.queue.FlowFileQueue;

/**
 * Defines a mechanism by which FlowFiles can be move into external storage or
 * memory so that they can be removed from the Java heap and vice-versa
 */
// TODO: This needs to be refactored into two different mechanisms, one that is responsible for doing
// framework-y types of things, such as updating the repositories, and another that is responsible
// for serializing and deserializing FlowFiles to external storage.
public interface FlowFileSwapManager {

    /**
     * Initializes the Swap Manager, providing a {@link SwapManagerInitializationContext} so that the
     * Swap Manager has access to all of the components necessary to perform its functions
     *
     * @param initializationContext the context the provides the swap manager with access to the
     *            resources that it needs to perform its functions
     */
    void initialize(SwapManagerInitializationContext initializationContext);

    /**
     * Swaps out the given FlowFiles that belong to the queue with the given identifier.
     *
     * @param flowFiles the FlowFiles to swap out to external storage
     * @param flowFileQueue the queue that the FlowFiles belong to
     * @return the location of the externally stored swap file
     *
     * @throws IOException if unable to swap the FlowFiles out
     */
    String swapOut(List<FlowFileRecord> flowFiles, FlowFileQueue flowFileQueue) throws IOException;

    /**
     * Recovers the SwapFiles from the swap file that lives at the given location. This action
     * provides a view of the FlowFiles but does not actively swap them in, meaning that the swap file
     * at the given location remains in that location and the FlowFile Repository is not updated.
     *
     * @param swapLocation the location of the swap file
     * @param flowFileQueue the queue that the FlowFiles belong to
     * @return the FlowFiles that live at the given swap location
     *
     * @throws IOException if unable to recover the FlowFiles from the given location
     */
    List<FlowFileRecord> peek(String swapLocation, FlowFileQueue flowFileQueue) throws IOException;

    /**
     * Recovers the FlowFiles from the swap file that lives at the given location and belongs
     * to the FlowFile Queue with the given identifier. The FlowFile Repository is then updated
     * and the swap file is permanently removed from the external storage
     *
     * @param swapLocation the location of the swap file
     * @param flowFileQueue the queue to which the FlowFiles belong
     *
     * @return the FlowFiles that are stored in the given location
     *
     * @throws IOException if unable to recover the FlowFiles from the given location or update the
     *             FlowFileRepository
     */
    List<FlowFileRecord> swapIn(String swapLocation, FlowFileQueue flowFileQueue) throws IOException;

    /**
     * Determines swap files that exist for the given FlowFileQueue
     *
     * @param flowFileQueue the queue for which the FlowFiles should be recovered
     *
     * @return all swap locations that have been identified for the given queue, in the order that they should
     *         be swapped back in
     */
    List<String> recoverSwapLocations(FlowFileQueue flowFileQueue) throws IOException;

    /**
     * Parses the contents of the swap file at the given location and provides a SwapSummary that provides
     * pertinent information about the information stored within the swap file
     *
     * @param swapLocation the location of the swap file
     * @return a SwapSummary that provides information about what is contained within the swap file
     * @throws IOException if unable to read or parse the swap file
     */
    SwapSummary getSwapSummary(String swapLocation) throws IOException;

    /**
     * Purge all known Swap Files without updating FlowFileRepository or Provenance Repository
     */
    void purge();
}
