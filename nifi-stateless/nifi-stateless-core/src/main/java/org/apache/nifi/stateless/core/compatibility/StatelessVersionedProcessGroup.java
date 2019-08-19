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
package org.apache.nifi.stateless.core.compatibility;

import org.apache.nifi.registry.flow.VersionedProcessGroup;

import java.util.Set;
import java.util.stream.Collectors;

public class StatelessVersionedProcessGroup implements StatelessProcessGroup {

    private final VersionedProcessGroup processGroup;

    public StatelessVersionedProcessGroup(final VersionedProcessGroup processGroup) {
        this.processGroup = processGroup;
    }

    @Override
    public String getId() {
        return this.processGroup.getIdentifier();
    }

    @Override
    public Set<? extends StatelessProcessGroup> getProcessGroups() {
        return this.processGroup.getProcessGroups().stream()
                .map(StatelessVersionedProcessGroup::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessProcessor> getProcessors() {
        return this.processGroup.getProcessors().stream()
                .map(StatelessVersionedProcessor::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessConnection> getConnections() {
        return this.processGroup.getConnections().stream()
                .map(StatelessVersionedConnection::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessRemoteProcessGroup> getRemoteProcessGroups() {
        return this.processGroup.getRemoteProcessGroups().stream()
                .map(StatelessVersionedRemoteProcessGroup::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessPort> getInputPorts() {
        return this.processGroup.getInputPorts().stream()
                .map(StatelessVersionedPort::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessPort> getOutputPorts() {
        return this.processGroup.getOutputPorts().stream()
                .map(StatelessVersionedPort::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessControllerService> getControllerServices() {
        return this.processGroup.getControllerServices().stream()
                .map(StatelessVersionedControllerService::new)
                .collect(Collectors.toSet());
    }
}
