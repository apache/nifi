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
package org.apache.nifi.tests.system;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

public class InstanceConfiguration {
    private final File bootstrapConfigFile;
    private final File instanceDirectory;
    private final File flowXmlGz;
    private final File stateDirectory;
    private final boolean autoStart;
    private final Map<String, String> nifiPropertiesOverrides;

    private InstanceConfiguration(Builder builder) {
        this.bootstrapConfigFile = builder.bootstrapConfigFile;
        this.instanceDirectory = builder.instanceDirectory;
        this.flowXmlGz = builder.flowXmlGz;
        this.stateDirectory = builder.stateDirectory;
        this.autoStart = builder.autoStart;
        this.nifiPropertiesOverrides = builder.nifiPropertiesOverrides;
    }

    public File getBootstrapConfigFile() {
        return bootstrapConfigFile;
    }

    public File getInstanceDirectory() {
        return instanceDirectory;
    }

    public File getFlowXmlGz() {
        return flowXmlGz;
    }

    public File getStateDirectory() {
        return stateDirectory;
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public Map<String, String> getNifiPropertiesOverrides() {
        return nifiPropertiesOverrides;
    }

    public static class Builder {
        private File bootstrapConfigFile;
        private File instanceDirectory;
        private File flowXmlGz;
        private File stateDirectory;
        private boolean autoStart = true;
        private final Map<String, String> nifiPropertiesOverrides = new HashMap<>();

        public Builder overrideNifiProperties(final Map<String, String> overrides) {
            nifiPropertiesOverrides.clear();
            if (overrides != null) {
                nifiPropertiesOverrides.putAll(overrides);
            }

            return this;
        }

        public Builder bootstrapConfig(final File configFile) {
            if (!configFile.exists()) {
                throw new RuntimeException(new FileNotFoundException(configFile.getAbsolutePath()));
            }

            this.bootstrapConfigFile = configFile;
            return this;
        }

        public Builder bootstrapConfig(final String configFilename) {
            return bootstrapConfig(new File(configFilename));
        }

        public Builder instanceDirectory(final File instanceDir) {
            this.instanceDirectory = instanceDir;
            return this;
        }

        public Builder instanceDirectory(final String instanceDirName) {
            return instanceDirectory(new File(instanceDirName));
        }

        public Builder flowXml(final File flowXml) {
            this.flowXmlGz = flowXml;
            return this;
        }

        public Builder flowXml(final String flowXmlFilename) {
            return flowXml(new File(flowXmlFilename));
        }

        public Builder stateDirectory(final File stateDirectory) {
            if (!stateDirectory.exists()) {
                throw new RuntimeException(new FileNotFoundException(stateDirectory.getAbsolutePath()));
            }

            if (!stateDirectory.isDirectory()) {
                throw new RuntimeException("Specified State Directory " + stateDirectory.getAbsolutePath() + " is not a directory");
            }

            this.stateDirectory = stateDirectory;
            return this;
        }

        public Builder stateDirectory(final String stateDirectoryName) {
            return stateDirectory(new File(stateDirectoryName));
        }

        public Builder autoStart(boolean autoStart) {
            this.autoStart = autoStart;
            return this;
        }

        public InstanceConfiguration build() {
            if (instanceDirectory == null) {
                throw new IllegalStateException("Instance Directory has not been specified");
            }
            if (bootstrapConfigFile == null) {
                throw new IllegalStateException("Bootstrap Config File has not been specified");
            }

            return new InstanceConfiguration(this);
        }
    }
}
