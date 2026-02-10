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

package org.apache.nifi.components.connector;

import org.apache.nifi.flow.VersionedConfigurationStep;

import java.util.List;

/**
 * Represents the externally managed working configuration of a Connector,
 * including its name and working flow configuration steps.
 *
 * <p>This is a mutable POJO used as the input/output for {@link ConnectorConfigurationProvider}
 * operations. The style follows the same pattern as {@link VersionedConfigurationStep} and other
 * versioned types in the NiFi API.</p>
 */
public class ConnectorWorkingConfiguration {
    private String name;
    private List<VersionedConfigurationStep> workingFlowConfiguration;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public List<VersionedConfigurationStep> getWorkingFlowConfiguration() {
        return workingFlowConfiguration;
    }

    public void setWorkingFlowConfiguration(final List<VersionedConfigurationStep> workingFlowConfiguration) {
        this.workingFlowConfiguration = workingFlowConfiguration;
    }
}
