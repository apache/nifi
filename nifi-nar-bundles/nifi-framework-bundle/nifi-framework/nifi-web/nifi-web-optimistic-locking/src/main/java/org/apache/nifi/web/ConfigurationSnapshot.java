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
 * Response object that captures the context of a given configuration.
 *
 * @param <T> type of snapshot
 */
public class ConfigurationSnapshot<T> {

    private final Long version;
    private final T configuration;
    private final boolean isNew;

    /**
     * Creates a new ConfigurationSnapshot with no configuration object.
     *
     * @param version The revision version
     */
    public ConfigurationSnapshot(Long version) {
        this(version, null);
    }

    /**
     * Creates a new ConfigurationSnapshot with the specified configuration
     * object. The object is consider to have been existing prior to this
     * request.
     *
     * @param version The revision version
     * @param configuration The configuration
     */
    public ConfigurationSnapshot(Long version, T configuration) {
        this(version, configuration, false);
    }

    /**
     * Creates a new ConfigurationSnapshot with the specified configuration
     * object. The isNew parameter specifies whether the object was created as a
     * result of this request.
     *
     * @param version       The revision version
     * @param configuration The configuration
     * @param isNew         Whether the resource was newly created
     */
    public ConfigurationSnapshot(Long version, T configuration, boolean isNew) {
        this.version = version;
        this.configuration = configuration;
        this.isNew = isNew;
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

    /**
     * Returns whether the configuration object was created as a result of this
     * request.
     *
     * @return Whether the object was created as a result of this request
     */
    public boolean isNew() {
        return isNew;
    }

}
