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
package org.apache.nifi.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MissingBundleException;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;

/**
 * Interface to define service methods for FlowController configuration.
 */
public interface FlowConfigurationDAO {

    /**
     * @return <code>true</code> if a file containing the flow is present, <code>false</code> otherwise
     */
    boolean isFlowPresent();

    /**
     * Loads the given controller with the values from the given proposed flow. If loading the proposed flow configuration would cause the controller to orphan flow files, then an
     * UninheritableFlowException is thrown.
     *
     * If the FlowSynchronizationException is thrown, then the controller may have changed some of its state and should no longer be used.
     *
     * @param controller a controller
     * @param dataFlow the flow to load
     * @throws java.io.IOException
     *
     * @throws FlowSerializationException if proposed flow is not a valid flow configuration file
     * @throws UninheritableFlowException if the proposed flow cannot be loaded by the controller because in doing so would risk orphaning flow files
     * @throws FlowSynchronizationException if updates to the controller failed. If this exception is thrown, then the controller should be considered unsafe to be used
     * @throws MissingBundleException if the proposed flow cannot be loaded by the controller because it contains a bundle that does not exist in the controller
     */
    void load(FlowController controller, DataFlow dataFlow)
            throws IOException, FlowSerializationException, FlowSynchronizationException, UninheritableFlowException, MissingBundleException;

    /**
     * Loads the stored flow onto the given stream.
     *
     * @param os an output stream
     * @throws IOException if unable to load the flow
     */
    void load(OutputStream os) throws IOException;

    /**
     * Loads the stored flow into the given stream, optionally compressed
     *
     * @param os the Output Stream to write the flow to
     * @param compressed whether or not the data should be gzipped
     * @throws IOException if unable to load the flow
     */
    void load(OutputStream os, boolean compressed) throws IOException;

    /**
     * Saves the given stream as the stored flow.
     *
     * @param is an input stream
     * @throws IOException if unable to save the flow
     */
    void save(InputStream is) throws IOException;

    /**
     * Saves all changes made to the given flow to the given File.
     *
     * @param flow to save
     * @throws NullPointerException if the given flow is null
     * @throws IOException If unable to persist state of given flow
     * @throws IllegalStateException if FileFlowDAO not in proper state for saving
     */
    void save(FlowController flow) throws IOException;

    /**
     * Saves all changes made to the given flow to the given File.
     *
     * @param flow to save
     * @param outStream the OutputStream to which the FlowController will be written
     * @throws NullPointerException if the given flow is null
     * @throws IOException If unable to persist state of given flow
     * @throws IllegalStateException if FileFlowDAO not in proper state for saving
     */
    void save(FlowController flow, OutputStream outStream) throws IOException;

    /**
     * Saves all changes made to the given flow to the given File.
     *
     * @param flow to save
     * @param archive if true will also attempt to archive the flow configuration
     * @throws NullPointerException if the given flow is null
     * @throws IOException If unable to persist state of given flow
     * @throws IllegalStateException if FileFlowDAO not in proper state for saving
     */
    void save(FlowController flow, boolean archive) throws IOException;

}
