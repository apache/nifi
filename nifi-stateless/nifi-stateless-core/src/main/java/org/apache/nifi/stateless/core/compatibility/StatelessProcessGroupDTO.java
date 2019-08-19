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

import org.apache.nifi.web.api.dto.ProcessGroupDTO;

import java.util.Set;
import java.util.stream.Collectors;

public class StatelessProcessGroupDTO implements StatelessProcessGroup {

    private final ProcessGroupDTO processGroup;

    public StatelessProcessGroupDTO(final ProcessGroupDTO processGroup) {
        this.processGroup = processGroup;
    }

    @Override
    public String getId() {
        return this.processGroup.getId();
    }

    @Override
    public Set<? extends StatelessProcessGroupDTO> getProcessGroups() {
        return this.processGroup.getContents().getProcessGroups()
                .stream()
                .map(StatelessProcessGroupDTO::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessProcessorDTO> getProcessors() {
        return this.processGroup.getContents().getProcessors()
                .stream()
                .map(StatelessProcessorDTO::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessConnectionDTO> getConnections() {
        return this.processGroup.getContents().getConnections()
                .stream()
                .map(StatelessConnectionDTO::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessRemoteProcessGroupDTO> getRemoteProcessGroups() {
        return this.processGroup.getContents().getRemoteProcessGroups()
                .stream()
                .map(StatelessRemoteProcessGroupDTO::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessPortDTO> getInputPorts() {
        return this.processGroup.getContents().getInputPorts()
                .stream()
                .map(StatelessPortDTO::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessPortDTO> getOutputPorts() {
        return this.processGroup.getContents().getOutputPorts()
                .stream()
                .map(StatelessPortDTO::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessControllerServiceDTO> getControllerServices() {
        return this.processGroup.getContents().getControllerServices()
                .stream()
                .map(StatelessControllerServiceDTO::new)
                .collect(Collectors.toSet());
    }
}
