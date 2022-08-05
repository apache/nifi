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

import org.apache.nifi.properties.ApplicationProperties;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.util.NiFiProperties;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolve configuration files that need to be encrypted from a given ApplicationProperties
 */
public class ConfigurationFileResolver {

    Path confDirectory;

    public ConfigurationFileResolver(final Path confDirectory) {
        this.confDirectory = confDirectory;
    }

    /**
     * Use the nifi.properties file to locate configuration files referenced by properties in the file
     *
     * @return List of application configuration files
     */
    public List<File> resolveConfigurationFilesFromApplicationProperties(final ApplicationProperties applicationProperties) throws ConfigurationFileResolverException {
        ArrayList<File> configurationFiles = new ArrayList<>();
        if (applicationProperties instanceof NiFiProperties) {
            NiFiProperties nifiProperties = (NiFiProperties) applicationProperties;
            configurationFiles.add(getAbsolute(nifiProperties.getAuthorizerConfigurationFile()));
            configurationFiles.add(getAbsolute(nifiProperties.getLoginIdentityProviderConfigurationFile()));
            configurationFiles.add(getAbsolute(nifiProperties.getStateManagementConfigFile()));
        } else if (applicationProperties instanceof NiFiRegistryProperties) {
            NiFiRegistryProperties nifiRegistryProperties = (NiFiRegistryProperties) applicationProperties;
            configurationFiles.add(getAbsolute(nifiRegistryProperties.getAuthorizersConfigurationFile()));
            configurationFiles.add(getAbsolute(nifiRegistryProperties.getProvidersConfigurationFile()));
            configurationFiles.add(getAbsolute(nifiRegistryProperties.getIdentityProviderConfigurationFile()));
            configurationFiles.add(getAbsolute(nifiRegistryProperties.getRegistryAliasConfigurationFile()));
        }

        for (final File configFile : configurationFiles) {
            if (!isValidConfigurationFile(configFile)) {
                throw new ConfigurationFileResolverException(String.format("Failed to resolve configuration file [%s].", configFile.getName()));
            }
        }

        return configurationFiles;
    }

    private boolean isValidConfigurationFile(final File configurationFile) throws ConfigurationFileResolverException {
        return configurationFile.isFile() && configurationFile.canRead();
    }

    public File resolveFlowDefinitionFileFromApplicationProperties(final NiFiProperties nifiProperties) {
        final File flowConfigurationFile = nifiProperties.getFlowConfigurationFile();
        if (flowConfigurationFile != null) {
            return flowConfigurationFile;
        } else {
            throw new ConfigurationFileResolverException("Failed to find a flow.xml.gz/flow.json.gz file");
        }
    }

    /**
     * Return a configuration file absolute path based on the confDirectory rather than Java's working path
     */
    private File getAbsolute(final File relativeFile) {
        if (relativeFile.isAbsolute() ) {
            return relativeFile;
        } else {
            return new File(confDirectory.getParent().toString(), relativeFile.getPath());
        }
    }
}
