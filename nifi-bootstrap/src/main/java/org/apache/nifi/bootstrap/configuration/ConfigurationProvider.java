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
package org.apache.nifi.bootstrap.configuration;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Abstraction for access to application configuration properties
 */
public interface ConfigurationProvider {
    /**
     * Get additional arguments for application command
     *
     * @return Additional arguments
     */
    List<String> getAdditionalArguments();

    /**
     * Get file containing application properties
     *
     * @return Application properties
     */
    Path getApplicationProperties();

    /**
     * Get file containing bootstrap configuration
     *
     * @return Bootstrap configuration
     */
    Path getBootstrapConfiguration();

    /**
     * Get directory containing application configuration
     *
     * @return Configuration directory
     */
    Path getConfigurationDirectory();

    /**
     * Get directory containing application libraries
     *
     * @return Library directory
     */
    Path getLibraryDirectory();

    /**
     * Get directory containing logs
     *
     * @return Log directory
     */
    Path getLogDirectory();

    /**
     * Get timeout configured for graceful shutdown of application process
     *
     * @return Graceful Shutdown Timeout duration
     */
    Duration getGracefulShutdownTimeout();

    /**
     * Get Management Server Address from the bootstrap configuration
     *
     * @return Management Server Address or empty when not configured
     */
    Optional<URI> getManagementServerAddress();

    /**
     * Get directory for current operations and resolving relative paths
     *
     * @return Working directory
     */
    Path getWorkingDirectory();
}
