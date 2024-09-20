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

package org.apache.nifi.python.processor;

import org.apache.nifi.components.AsyncLoadedProcessor.LoadState;
import org.apache.nifi.python.PythonController;

import java.util.Optional;

/**
 * A model object that is used to bridge the gap between what is necessary for the Framework to interact
 * with a Python Processor and what is made available by lower level APIs
 */
public interface PythonProcessorBridge {
    /**
     * @return a proxy for the PythonProcessorAdapter that is responsible for calling the associated method on the Python side,
     * or an empty Optional if the adapter has not yet been initialized
     */
    Optional<PythonProcessorAdapter> getProcessorAdapter();

    void replaceController(PythonController controller);

    /**
     * @return the name of the Processor implementation.
     */
    String getProcessorType();

    /**
     * Reloads the processor from source code, so that the next invocation of any method that's defined by the Processor will invoke
     * the new implementation. Note that the Processor will not be reloaded if the source has not changed since the Processor was last loaded.
     *
     * @return <code>true</code> if reloaded, <code>false</code> if not
     */
    boolean reload();

    /**
     * Initializes the Processor
     * @param context the initialization context
     */
    void initialize(PythonProcessorInitializationContext context);

    /**
     * @return the current state of the Processor loading
     */
    LoadState getLoadState();
}
