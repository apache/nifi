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

package org.apache.nifi.minifi.c2.service;

import org.apache.nifi.minifi.c2.api.Configuration;
import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;

import javax.ws.rs.core.MediaType;

public class ConfigurationProviderValue {
    private final Configuration configuration;
    private final MediaType mediaType;
    private final ConfigurationProviderException configurationProviderException;

    public ConfigurationProviderValue(Configuration configuration, MediaType mediaType, ConfigurationProviderException configurationProviderException) {
        this.configuration = configuration;
        this.mediaType = mediaType;
        this.configurationProviderException = configurationProviderException;
    }

    public Configuration getConfiguration() throws ConfigurationProviderException {
        if (configurationProviderException != null) {
            throw configurationProviderException;
        }
        return configuration;
    }

    public MediaType getMediaType() throws ConfigurationProviderException {
        if (configurationProviderException != null) {
            throw configurationProviderException;
        }
        return mediaType;
    }
}
