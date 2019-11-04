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
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public interface NiFiInstance {

    /**
     * Sets up the environment for the instance to run in
     */
    void createEnvironment() throws IOException;

    default void start() {
        start(true);
    }

    /**
     * Starts the NiFi instance and waits until the startup is complete
     */
    void start(boolean waitForCompletion);

    /**
     * Shuts down the NiFi instance
     */
    void stop();

    /**
     * @return <code>true</code> if this node is part of a cluster (regardless of whether or not its current state is connected), <code>false</code> if the instance is a standalone instance
     */
    boolean isClustered();

    /**
     * @return the number of nodes in the cluster if this instance is part of a cluster, <code>1</code> otherwise
     */
    int getNumberOfNodes();

    /**
     * Returns the number of nodes in the cluster, optionally excluding instances that are not set to auto-start.
     *
     * @param includeOnlyAutoStartInstances whether or not to include instances that should not auto-start
     *
     * @return the number of nodes in the cluster
     */
    int getNumberOfNodes(boolean includeOnlyAutoStartInstances);

    /**
     * Returns the NiFiInstance for a specific node
     *
     * @param nodeIndex the 1-based index of the node
     *
     * @return the NiFi instance for the specified node
     *
     * @throws UnsupportedOperationException if the NiFi instance is not clustered
     */
    NiFiInstance getNodeInstance(int nodeIndex);

    /**
     * Returns the NiFiProperties for the node
     *
     * @return the nifi properties for the node
     */
    Properties getProperties() throws IOException;

    /**
     * @return the root directory for the instance
     */
    File getInstanceDirectory();

    /**
     * @return Whether or not this instance should automatically start when the test is run.
     */
    boolean isAutoStart();

    /**
     * Change the value of one of the properties in nifi.properties. If the node is already running, this change will not take effect until the instance is stopped and started again.
     *
     * @param propertyName the name of the property
     * @param propertyValue the value of the property
     */
    void setProperty(String propertyName, String propertyValue) throws IOException;

    /**
     * Change the value of the flow that should be loaded on startup
     * @param flowXmlGz the file that contains the flow that should be loaded on startup
     */
    void setFlowXmlGz(final File flowXmlGz) throws IOException;

    /**
     * Change the values of the given properties in nifi.properties. Any property that is not present in the given map will remain unchanged. If the node is already running, this change will not take
     * effect until the instance is stopped and started again.
     *
     * @param properties the properties to change
     */
    void setProperties(Map<String, String> properties) throws IOException;
}
