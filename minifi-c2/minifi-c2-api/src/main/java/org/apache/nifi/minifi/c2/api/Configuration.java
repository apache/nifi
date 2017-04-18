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

package org.apache.nifi.minifi.c2.api;

import java.io.InputStream;

/**
 * Represents a MiNiFi configuration of a given version, format matches the format of the ConfigurationProvider
 *
 * This object may be cached so it should attempt to minimize the amount of memory used to represent state (input stream should come from persistent storage if possible.)
 */
public interface Configuration {
    /**
     * Gets the version
     *
     * @return the version
     */
    String getVersion();

    /**
     * Returns a boolean indicating whether this version exists
     *
     * @return a boolean indicating whether this version exists
     */
    boolean exists();
    /**
     * Returns an input stream to read the configuration with
     *
     * @return an input stream to read the configuration with
     */
    InputStream getInputStream() throws ConfigurationProviderException;
}
