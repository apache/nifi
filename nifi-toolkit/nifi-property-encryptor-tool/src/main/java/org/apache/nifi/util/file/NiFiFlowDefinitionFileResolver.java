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
package org.apache.nifi.util.file;

import org.apache.nifi.util.NiFiProperties;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolve a NiFi flow definition file that needs to be encrypted from the provided NiFiProperties file
 */
public class NiFiFlowDefinitionFileResolver implements ConfigurationFileResolver<NiFiProperties> {

    public NiFiFlowDefinitionFileResolver() {
    }

    /**
     * Use the nifi.properties file to locate a flow definition file (flow.xml.gz/flow.json.gz)
     *
     * @return Flow definition file
     */
    public List<File> resolveFilesFromApplicationProperties(final NiFiProperties properties) throws ConfigurationFileResolverException {
        ArrayList<File> configurationFiles = new ArrayList<>();

        final File flowConfigurationFile = properties.getFlowConfigurationFile();
        if (flowConfigurationFile != null) {
            configurationFiles.add(flowConfigurationFile);
            return configurationFiles;
        } else {
            throw new ConfigurationFileResolverException("Failed to find a flow.xml.gz/flow.json.gz file");
        }
    }
}
