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

import org.apache.nifi.minifi.c2.api.ConfigurationProvider;
import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.util.Pair;

import javax.ws.rs.core.MediaType;
import java.util.List;

public class ConfigurationProviderInfo {
    private final List<Pair<MediaType, ConfigurationProvider>> mediaTypeList;
    private final List<String> contentTypes;
    private final ConfigurationProviderException configurationProviderException;

    public ConfigurationProviderInfo(List<Pair<MediaType, ConfigurationProvider>> mediaTypeList, List<String> contentTypes, ConfigurationProviderException configurationProviderException) {
        this.mediaTypeList = mediaTypeList;
        this.contentTypes = contentTypes;
        this.configurationProviderException = configurationProviderException;
    }

    public List<Pair<MediaType, ConfigurationProvider>> getMediaTypeList() throws ConfigurationProviderException {
        if (configurationProviderException != null) {
            throw configurationProviderException;
        }
        return mediaTypeList;
    }

    public List<String> getContentTypes() throws ConfigurationProviderException {
        if (configurationProviderException != null) {
            throw configurationProviderException;
        }
        return contentTypes;
    }
}
