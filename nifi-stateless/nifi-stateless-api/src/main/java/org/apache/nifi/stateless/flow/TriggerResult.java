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

package org.apache.nifi.stateless.flow;

import org.apache.nifi.flowfile.FlowFile;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TriggerResult {

    /**
     * @return <code>true</code> if the dataflow completed successfully, <code>false</code> if the dataflow failed to run to completion successfully
     */
    boolean isSuccessful();

    /**
     * @return <code>true</code> if the dataflow execution was canceled, <code>false</code> otherwise
     */
    boolean isCanceled();

    /**
     * If the dataflow failed to run to completion, returns the Exception that caused the failure
     * @return the Exception that caused the dataflow to fail, or an empty Optional if there was no Exception thrown
     */
    Optional<Throwable> getFailureCause();

    /**
     * @return a mapping of Output Port Name to all FlowFiles that were transferred to that Output Port
     */
    Map<String, List<FlowFile>> getOutputFlowFiles();

    /**
     * Returns a List of all FlowFiles that were transferred to the Output Port with the given name
     * @param portName the name of the Output Port
     * @return a List all FlowFiles that were transferred to the Output Port. Will return an empty list if no FlowFiles transferred.
     */
    List<FlowFile> getOutputFlowFiles(String portName);

    /**
     * Provides the contents of a FlowFile that was obtained by calling {@link #getOutputFlowFiles()}.
     * @param flowFile the FlowFile whose contents are to be read
     * @return the contents of the FlowFile
     */
    byte[] readContent(FlowFile flowFile);

    /**
     * Acknowledges the output of the dataflow and allows the session to be successfully committed.
     */
    void acknowledge();
}
