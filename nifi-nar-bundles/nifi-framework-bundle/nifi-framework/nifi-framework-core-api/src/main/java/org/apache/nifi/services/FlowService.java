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
package org.apache.nifi.services;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.lifecycle.LifeCycle;

/**
 * Defines the API level services available for carrying out file-based dataflow operations.
 *
 */
public interface FlowService extends LifeCycle {

    /**
     * Immediately persists the state of the flow controller to the flow.xml file in a blocking call.
     *
     * @throws NullPointerException if the given flow is null.
     * @throws IOException if any problem occurs creating/modifying file
     */
    void saveFlowChanges() throws IOException;

    /**
     * Immediately persists the state of the flow controller to the given output stream in a blocking call.
     *
     * @param outStream the stream to which the FlowController is to be persisted
     * @throws NullPointerException if the given flow is null.
     * @throws IOException if any problem occurs creating/modifying file
     */
    void saveFlowChanges(OutputStream outStream) throws IOException;

    /**
     * Saves the given stream to the flow.xml file on disk. This method does not change the state of the flow controller.
     *
     * @param is an input stream
     * @throws IOException if unable to save the flow
     */
    void overwriteFlow(InputStream is) throws IOException;

    /**
     * Asynchronously saves the flow controller. The flow controller will be copied and immediately returned. If another call to save is made within that time the latest called state of the flow
     * controller will be used. In database terms this technique is referred to as 'write-delay'.
     *
     * @param delayUnit unit of delay
     * @param delay period of delay
     */
    void saveFlowChanges(TimeUnit delayUnit, long delay);

    /**
     * Asynchronously saves the flow controller. The flow controller will be copied and immediately returned. If another call to save is made within that time the latest called state of the flow
     * controller will be used. In database terms this technique is referred to as 'write-delay'.
     *
     * @param delayUnit unit of delay
     * @param delay period of delay
     * @param archive if true means the user wants the flow configuration to be archived as well
     */
    void saveFlowChanges(TimeUnit delayUnit, long delay, boolean archive);

    /**
     * Stops the flow and underlying repository as determined by user
     *
     * @param force if true the controller is not allowed to gracefully shut down.
     */
    @Override
    void stop(boolean force);

    /**
     * Loads the flow controller with the given flow. Passing null means that the local flow on disk will used as the proposed flow. If loading the proposed flow configuration would cause the
     * controller to orphan flow files, then an UninheritableFlowException is thrown.
     *
     * If the FlowSynchronizationException is thrown, then the controller may have changed some of its state and should no longer be used.
     *
     * @param proposedFlow the flow to load
     *
     * @throws IOException if flow configuration could not be retrieved from disk
     */
    void load(DataFlow proposedFlow) throws IOException;

    /**
     * Copies the contents of the current flow.xml to the given OutputStream
     *
     * @param os an output stream
     * @throws IOException if unable to load the flow
     */
    void copyCurrentFlow(OutputStream os) throws IOException;

    /**
     * Creates a DataFlow object by first looking for a flow on from disk, and falling back to the controller's flow otherwise.
     *
     * @return the created DataFlow object
     *
     * @throws IOException if unable to read the flow from disk
     */
    DataFlow createDataFlow() throws IOException;

    /**
     * Creates a DataFlow object by serializing the flow controller's flow.
     *
     * @return the created DataFlow object.
     */
    DataFlow createDataFlowFromController() throws IOException;

}
