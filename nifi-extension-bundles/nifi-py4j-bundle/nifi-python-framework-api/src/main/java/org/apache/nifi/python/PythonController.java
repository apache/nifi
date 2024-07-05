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

import org.apache.nifi.python.processor.PreserveJavaBinding;
import org.apache.nifi.python.processor.PythonProcessorAdapter;

import java.util.List;

/**
 * An interface that is implemented on the Python side. All method invocations are to be proxied via Py4J.
 */
public interface PythonController {
    /**
     * A simple test method. The return value of this method should be the literal string value <code>pong</code>. Anything else
     * indicates that the Python side is not functioning properly.
     */
    String ping();

    /**
     * @return a list of PythonProcessorDetails representing the processors that are available on the Python side
     */
    List<PythonProcessorDetails> getProcessorTypes();

    /**
     * Discovers any extensions that are available in the given directories, and imports any dependencies that they may have,
     * using the given work directory
     *
     * @param directories the directories to scan for extensions
     * @param workDirectory the directory to install third party dependencies in, etc.
     */
    void discoverExtensions(List<String> directories, String workDirectory);


    /**
     * Downloads any third-party dependencies that are necessary in order to use the processor of the given type and version
     *
     * @param type          the type of the processor
     * @param version       the version of the processor
     * @param workDirectory the work directory where the processor's virtual environment should live
     */
    void downloadDependencies(String type, String version, String workDirectory);

    /**
     * Creates a Processor of the given type and version, returning a PythonProcessorAdapter that can be used for interacting with it.
     *
     * @param type the type of the Processor
     * @param version the version of the Processor
     * @param workDirectory the work directory
     * @return a PythonProcessorAdapter that can be used for interacting with the Python Processor
     */
    PythonProcessorAdapter createProcessor(String type, String version, String workDirectory);

    /**
     * Reloads the Processor's implementation
     *
     * @param type the type of the Processor
     * @param version the version
     * @param workDirectory the working directory for storing third-party dependencies, etc.
     */
    void reloadProcessor(String type, String version, String workDirectory);

    /**
     * Removes the given Processor from the Python side.
     *
     * @param type the type of the Processor
     * @param version the version
     */
    void removeProcessorType(String type, String version);

    /**
     * Setter to provide the ContorllerServiceTypeLookup, which is used to allow Python Processors to make use of
     * Controller Services.
     * @param lookup the lookup
     */
    @PreserveJavaBinding
    void setControllerServiceTypeLookup(ControllerServiceTypeLookup lookup);

    /**
     * Returns the fully qualified path name to the Python module file that provides the processor with the given type and version
     * @param processorType the type of the Processor
     * @param version the version of the Processor
     * @return the fully qualified path name to the Python module
     */
    String getModuleFile(String processorType, String version);

    /**
     * Returns details about the Processor with the given type and version
     *
     * @param type    the type of the Processor
     * @param version the version of the Processor
     * @return the details that have been discovered
     */
    PythonProcessorDetails getProcessorDetails(String type, String version);

    /**
     * Set level for specified logger name in Python logging.Logger objects
     *
     * @param loggerName Python logger name to be updated
     * @param level Python log level according to Python logging module documentation
     */
    void setLoggerLevel(String loggerName, int level);
}
