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
package org.apache.nifi.web;

/**
 * Response object that captures some configuration for a given revision.
 * @param <T>
 */
public class ConfigurationSnapshot<T> {

    private Long version;
    private T configuration;

    /**
     * Creates a new ConfigurationSnapshot.
     *
     * @param version The revision version
     */
    public ConfigurationSnapshot(Long version) {
        this(version, null);
    }

    /**
     * Creates a new ConfigurationSnapshot.
     *
     * @param version The revision version
     * @param configuration The configuration
     */
    public ConfigurationSnapshot(Long version, T configuration) {
        this.version = version;
        this.configuration = configuration;
    }

    /**
     * Get the revision version.
     *
     * @return The revision version
     */
    public Long getVersion() {
        return version;
    }

    /**
     * Get the configuration of the operation.
     *
     * @return The configuration of the operation
     */
    public T getConfiguration() {
        return configuration;
    }

}
