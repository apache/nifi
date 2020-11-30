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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CanceledTriggerResult implements TriggerResult {
    @Override
    public boolean isSuccessful() {
        return false;
    }

    @Override
    public boolean isCanceled() {
        return true;
    }

    @Override
    public Optional<Throwable> getFailureCause() {
        return Optional.empty();
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
        return new byte[0];
    }

    @Override
    public void acknowledge() {
    }
}
