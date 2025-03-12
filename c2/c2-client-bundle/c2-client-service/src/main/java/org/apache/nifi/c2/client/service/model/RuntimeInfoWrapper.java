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
package org.apache.nifi.c2.client.service.model;

import java.util.List;
import java.util.Map;
import org.apache.nifi.c2.protocol.api.AgentRepositories;
import org.apache.nifi.c2.protocol.api.FlowQueueStatus;
import org.apache.nifi.c2.protocol.api.ProcessorBulletin;
import org.apache.nifi.c2.protocol.api.ProcessorStatus;
import org.apache.nifi.c2.protocol.api.RunStatus;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;

public class RuntimeInfoWrapper {
    final AgentRepositories repos;
    final RuntimeManifest manifest;
    final Map<String, FlowQueueStatus> queueStatus;
    final List<ProcessorBulletin> processorBulletins;
    final List<ProcessorStatus> processorStatus;
    final RunStatus runStatus;

    public RuntimeInfoWrapper(AgentRepositories repos, RuntimeManifest manifest, Map<String, FlowQueueStatus> queueStatus, List<ProcessorBulletin> processorBulletins,
                              List<ProcessorStatus> processorStatus, RunStatus runStatus) {
        this.repos = repos;
        this.manifest = manifest;
        this.queueStatus = queueStatus;
        this.processorBulletins = processorBulletins;
        this.processorStatus = processorStatus;
        this.runStatus = runStatus;
    }

    public AgentRepositories getAgentRepositories() {
        return repos;
    }

    public RuntimeManifest getManifest() {
        return manifest;
    }

    public Map<String, FlowQueueStatus> getQueueStatus() {
        return queueStatus;
    }

    public List<ProcessorBulletin> getProcessorBulletins() {
        return processorBulletins;
    }

    public List<ProcessorStatus> getProcessorStatus() {
        return processorStatus;
    }

    public RunStatus getRunStatus() {
        return runStatus;
    }
}
