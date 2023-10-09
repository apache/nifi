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

import java.util.List;

public interface PythonProcessorDetails {
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
     * @return the name of the Python Package Index (PyPi) package, or <code>null</code> if it is not available
     */
    String getPyPiPackageName();

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
}
