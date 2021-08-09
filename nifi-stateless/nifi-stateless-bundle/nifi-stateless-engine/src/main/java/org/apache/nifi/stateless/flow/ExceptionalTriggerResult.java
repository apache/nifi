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
import org.apache.nifi.processor.exception.TerminatedTaskException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExceptionalTriggerResult implements TriggerResult {
    private final Throwable failureCause;

    public ExceptionalTriggerResult(final Throwable failureCause) {
        this.failureCause = failureCause;
    }

    @Override
    public boolean isSuccessful() {
        return false;
    }

    @Override
    public boolean isCanceled() {
        return failureCause instanceof TerminatedTaskException;
    }

    @Override
    public Optional<Throwable> getFailureCause() {
        return Optional.ofNullable(failureCause);
    }

    @Override
    public Map<String, List<FlowFile>> getOutputFlowFiles() {
        return Collections.emptyMap();
    }

    @Override
    public List<FlowFile> getOutputFlowFiles(final String portName) {
        return Collections.emptyList();
    }

    @Override
    public byte[] readContent(final FlowFile flowFile) {
        throw new IllegalArgumentException("Unknown FlowFile: " + flowFile);
    }

    @Override
    public void acknowledge() {
    }
}
