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

package org.apache.nifi.stateless.engine;

import org.apache.nifi.stateless.config.ExtensionClientDefinition;
import org.apache.nifi.stateless.config.SslContextDefinition;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface StatelessEngineConfiguration {
    /**
     * @return the directory to use for 'scratch space', such as unpacking NAR files
     */
    File getWorkingDirectory();

    /**
     * @return the directory containing the NiFi Stateless NAR and all other necessary libraries required for the Stateless engine to be bootstrapped
     */
    File getNarDirectory();

    /**
     * @return a directory containing extensions that should be loaded and into which extensions can be downloaded
     */
    File getExtensionsDirectory();

    /**
     * @return a collection of zero or more directories that contain extensions that should be loaded by the stateless engine. The engine will not download
     * any extensions into these directories or write to them but will read any NAR files that are found within these directories. The engine will not recurse
     * into subdirectories of these directories.
     */
    Collection<File> getReadOnlyExtensionsDirectories();

    /**
     * @return the KRB5 file to use for establishing any Kerberos connections, or <code>null</code> if no KRB5 file is to be used
     */
    File getKrb5File();

    /**
     * @return the directory to use for storing FlowFile Content, or an empty optional if content is to be stored in memory
     */
    Optional<File> getContentRepositoryDirectory();

    /**
     * @return the definition needed to create an SSL Context that can be used for interacting with a Nexus Repository or retrieving a flow from the Flow Registry, etc.
     * This SSL Context will NOT be made available to extensions running in the dataflow.
     */
    SslContextDefinition getSslContext();

    /**
     * @return a sensitive properties key that extensions may choose to use for encrypting/decrypting sensitive information
     */
    String getSensitivePropsKey();

    /**
     * @return a List of definitions for Extension Clients. These clients will be used to attempt to download any extensions that are required by a dataflow that are not
     * already present. The clients will be used in the order in which they are provided. Therefore, if one client is to be preferred over another, it should come first in
     * the List.
     */
    List<ExtensionClientDefinition> getExtensionClients();

    /**
     * When discovering extensions, indicates whether or not the discovered extensions should be logged at an INFO level.
     * @return <code>true</code> if the discovered extensions should be logged at an INFO level, <code>false</code> if they should not be logged (or logged only at DEBUG level).
     */
    default boolean isLogExtensionDiscovery() {
        return true;
    }

    /**
     * @return a String representing the interval between periodic status task executions (e.g., 1 min).
     * A <code>null</code> value indicates that no status tasks are scheduled.
     */
    String getStatusTaskInterval();

    /**
     * @return a String representing the length of time that the process scheduler should wait for a process to start
     * Defaults to "10 secs"
     */
    default String getProcessorStartTimeout() {
       return "10 secs";
    }

    /**
     * @return a String representing the length of time that the StatelessEngine should wait for a component to enable
     * Defaults to "10 secs"
     */
    default String getComponentEnableTimeout() {
        return "10 sec";
    }
}
