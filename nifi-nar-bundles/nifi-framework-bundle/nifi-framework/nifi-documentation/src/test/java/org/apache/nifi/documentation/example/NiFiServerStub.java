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
package org.apache.nifi.documentation.example;

import org.apache.nifi.NiFiServer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.controller.DecommissionTask;
import org.apache.nifi.diagnostics.DiagnosticsFactory;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.util.NiFiProperties;

import java.util.Set;

/**
 * This stub is the source code for the no-op NiFiServer implementation used in the nifiserver-test-nar.nar, as NiFi requires exactly one
 * implementation of NiFiServer in order to start successfully. The NAR was built externally, but the code is provided here in case
 * updates are needed.
 */
public class NiFiServerStub implements NiFiServer {
    @Override
    public void start() {

    }

    @Override
    public void initialize(NiFiProperties properties, Bundle systemBundle, Set<Bundle> bundles, ExtensionMapping extensionMapping) {

    }

    @Override
    public void stop() {

    }

    @Override
    public DiagnosticsFactory getDiagnosticsFactory() {
        return null;
    }

    @Override
    public DiagnosticsFactory getThreadDumpFactory() {
        return null;
    }

    @Override
    public DecommissionTask getDecommissionTask() {
        return null;
    }
}
