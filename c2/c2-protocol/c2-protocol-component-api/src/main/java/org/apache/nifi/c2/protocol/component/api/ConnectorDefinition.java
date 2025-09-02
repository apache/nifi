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
package org.apache.nifi.c2.protocol.component.api;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Collections;
import java.util.List;

/**
 * Definition of a Connector component.
 */
public class ConnectorDefinition extends ExtensionComponent {
    private static final long serialVersionUID = 1L;

    private List<ConfigurationStep> configurationSteps;

    @Schema(description = "The configuration steps for this connector")
    public List<ConfigurationStep> getConfigurationSteps() {
        return (configurationSteps != null ? Collections.unmodifiableList(configurationSteps) : null);
    }

    public void setConfigurationSteps(List<ConfigurationStep> configurationSteps) {
        this.configurationSteps = configurationSteps;
    }
}

