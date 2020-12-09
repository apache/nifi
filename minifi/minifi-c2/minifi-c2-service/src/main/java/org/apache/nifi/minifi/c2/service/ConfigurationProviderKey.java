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

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigurationProviderKey {
    private final List<MediaType> acceptValues;
    private final Map<String, List<String>> parameters;

    public ConfigurationProviderKey(List<MediaType> acceptValues, Map<String, List<String>> parameters) {
        this.acceptValues = Collections.unmodifiableList(new ArrayList<>(acceptValues));
        this.parameters = Collections.unmodifiableMap(parameters.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.unmodifiableList(new ArrayList<>(e.getValue())))));
    }

    public List<MediaType> getAcceptValues() {
        return acceptValues;
    }

    public Map<String, List<String>> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConfigurationProviderKey that = (ConfigurationProviderKey) o;

        if (!acceptValues.equals(that.acceptValues)) return false;
        return parameters.equals(that.parameters);
    }

    @Override
    public String toString() {
        return "ConfigurationProviderKey{" +
                "acceptValues=" + acceptValues +
                ", parameters=" + parameters +
                '}';
    }

    @Override
    public int hashCode() {
        int result = acceptValues.hashCode();
        result = 31 * result + parameters.hashCode();
        return result;
    }
}
