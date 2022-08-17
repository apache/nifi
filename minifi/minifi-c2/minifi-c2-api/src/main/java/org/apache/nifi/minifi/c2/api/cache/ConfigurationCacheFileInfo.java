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

package org.apache.nifi.minifi.c2.api.cache;

import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Information on the different versions of a cache entry
 */
public interface ConfigurationCacheFileInfo {
    /**
     * Returns the version the file would be assigned based on its name
     *
     * @param filename the filename
     * @return the version
     */
    Integer getVersionIfMatch(String filename);

    /**
     * Returns a stream of WritableConfigurations for this cache entry
     *
     * @return the stream
     * @throws IOException if there is an error getting the configurations
     */
    Stream<WriteableConfiguration> getCachedConfigurations() throws IOException;

    /**
     * Returns a WritableConfiguration for the given version of this cache entry
     *
     * @param version the version
     * @return the configuration
     * @throws ConfigurationProviderException if there is a problem getting the configuration
     */
    WriteableConfiguration getConfiguration(Integer version) throws ConfigurationProviderException;
}
