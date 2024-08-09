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
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An implementation of the PythonBridge that does not enable any interaction with Python or launch any Python Processes.
 */
public class DisabledPythonBridge implements PythonBridge {
    @Override
    public void initialize(final PythonBridgeInitializationContext context) {
    }

    @Override
    public void start() throws IOException {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void ping() {
    }

    @Override
    public List<PythonProcessorDetails> getProcessorTypes() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Integer> getProcessCountsPerType() {
        return Collections.emptyMap();
    }

    @Override
    public List<BoundObjectCounts> getBoundObjectCounts() {
        return Collections.emptyList();
    }

    @Override
    public void discoverExtensions(final boolean includeNarDirectories) {
    }

    @Override
    public void discoverExtensions(final List<File> extensionDirectories) {
    }

    @Override
    public AsyncLoadedProcessor createProcessor(final String identifier, final String type, final String version, final boolean preferIsolatedProcess, final boolean initialize) {
        throw new UnsupportedOperationException("Cannot create Processor of type " + type + " because Python extensions are disabled");
    }

    @Override
    public void onProcessorRemoved(final String identifier, final String type, String version) {
    }

    @Override
    public void removeProcessorType(final String type, final String version) {
    }
}
