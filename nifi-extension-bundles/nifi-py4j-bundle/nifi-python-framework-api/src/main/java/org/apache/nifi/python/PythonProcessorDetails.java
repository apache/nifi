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

import org.apache.nifi.python.processor.documentation.MultiProcessorUseCaseDetails;
import org.apache.nifi.python.processor.documentation.PropertyDescription;
import org.apache.nifi.python.processor.documentation.UseCaseDetails;

import java.util.List;

public interface PythonProcessorDetails extends PythonObjectProxy {
    /**
     * @return the type of the Processor (i.e., the class name of the Processor class)
     */
    String getProcessorType();

    /**
     * @return the version of the Processor
     */
    String getProcessorVersion();

    /**
     * @return the location of the Processor's source file, or <code>null</code> if it is not available.
     */
    String getSourceLocation();

    /**
     * @return the directory where the Processor's extension is installed. If the extension is a module, this will be the directory
     *         containing the module. If the extension is a single file outside of a module, this will be the directory containing
     *         that file.
     */
    String getExtensionHome();

    /**
     * @return <code>true</code> if the Processor is bundled with its dependencies; <code>false</code> otherwise
     */
    boolean isBundledWithDependencies();

    /**
     * @return the Processor's capability description
     */
    String getCapabilityDescription();

    /**
     * @return the tags that have been declared on the processor
     */
    List<String> getTags();

    /**
     * @return the dependencies that must be imported in order to use the Processor
     */
    List<String> getDependencies();

    /**
     * @return the name of the Java interface that is implemented by the Python Processor
     */
    String getInterface();

    /**
     * @return the use cases that have been described by the Python Processor
     */
    List<UseCaseDetails> getUseCases();

    /**
     * @return the multi-processor use cases that have been described by the Python Processor
     */
    List<MultiProcessorUseCaseDetails> getMultiProcessorUseCases();

    /**
     * A list of property descriptions for the known properties. Note that unlikely Java Processors, Python-based Processors are
     * more dynamic, and the properties may not all be discoverable
     *
     * @return a list of descriptions for known properties
     */
    List<PropertyDescription> getPropertyDescriptions();

    /**
     * @return the coordinate of the bundle/NAR that this processor was loaded from
     */
    PythonBundleCoordinate getBundleCoordinate();
}
