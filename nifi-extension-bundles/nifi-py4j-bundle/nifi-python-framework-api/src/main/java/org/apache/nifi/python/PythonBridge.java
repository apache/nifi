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

package org.apache.nifi.python;

import org.apache.nifi.components.AsyncLoadedProcessor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * The PythonBridge is the main gateway between NiFi's Java process and the Python processes.
 * The implementation is responsible for spawning Python Processes, tearing them down, and orchestrating
 * communication between them.
 * </p>
 *
 * <p>
 * All implementations must provide a default constructor.
 * </p>
 */
public interface PythonBridge {

    /**
     * Initializes the Python Bridge so that it may be started.
     * @param context the initialization context.
     */
    void initialize(PythonBridgeInitializationContext context);

    /**
     * Starts the Bridge so that Python Processes will be launched as necessary and interaction with the Python side
     * can occur
     */
    void start() throws IOException;

    /**
     * Shuts down the Bridge, terminating any launched Python Processes and cleaning up any resources that have been created
     */
    void shutdown();

    /**
     * A simple test to ensure that the Java side is capable of communicating with the Python process
     * @throws IOException if unable to communicate
     */
    void ping() throws IOException;

    /**
     * Returns a List of PythonProcessorDetails for each Processor that is available in the Python side. This method should not be called
     * until after calling {@link #discoverExtensions(boolean)}.
     *
     * @return a List of PythonProcessorDetails for each Processor that is available in the Python side
     */
    List<PythonProcessorDetails> getProcessorTypes();

    /**
     * @return a mapping of Processor type to the number of Python Processes currently running that processor type
     */
    Map<String, Integer> getProcessCountsPerType();

    /**
     * In order to allow the Python Process to interact with Java objects, we must have some way for the Python process
     * to identify the object. This mapping between Object ID and object is held within a 'Java Object Bindings'.
     * This determines the number of objects of each type that are currently bound for each Python Process.
     *
     * @return a mapping of class names to the number of objects of that type that are currently bound for each Process
     */
    List<BoundObjectCounts> getBoundObjectCounts();

    /**
     * Triggers the Python Bridge to scan in order to determine which extensions are available. The results may then be obtained by calling
     * {@link #getProcessorTypes()}.
     *
     * @param includeNarDirectories whether or not to include NAR directories in the search for extensions
     */
    void discoverExtensions(boolean includeNarDirectories);

    /**
     * Triggers the Python Bridge to scan the given directories in order to determine which extensions are available.
     *
     * @param extensionDirectories the extension directories to scan
     */
    void discoverExtensions(List<File> extensionDirectories);

    /**
     * Creates a Processor with the given identifier, type, and version.
     *
     * @param identifier the Processor's identifier
     * @param type the Processor's type
     * @param version the Processor's version
     * @param preferIsolatedProcess whether or not to prefer launching a Python Process that is isolated for just this one instance of the Processor
     * @param initialize whether or not to initialize the processor
     * @return a PythonProcessorBridge that can be used for interacting with the Processor
     */
    AsyncLoadedProcessor createProcessor(String identifier, String type, String version, boolean preferIsolatedProcess, boolean initialize);

    /**
     * A notification that the Processor with the given identifier, type, and version was removed from the flow. This triggers the bridge
     * to cleanup any necessary resources, including the Python Process that the Processor is running in.
     *
     * @param identifier the identifier of the removed Processor
     * @param type the type of the removed Processor
     * @param version the version of the removed Processor
     */
    void onProcessorRemoved(String identifier, String type, String version);

    /**
     * Removes the given processor from the Python side.
     *
     * @param type the processor type
     * @param version the processor version
     */
    void removeProcessorType(String type, String version);
}
