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
package org.apache.nifi.redis.state;

import org.apache.nifi.components.state.StateMap;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A StateMap implementation for RedisStateProvider.
 */
public class RedisStateMap implements StateMap {

    public static final Long DEFAULT_VERSION = new Long(-1);
    public static final Integer DEFAULT_ENCODING = new Integer(1);

    private final Long version;
    private final Integer encodingVersion;
    private final Map<String,String> stateValues;

    private RedisStateMap(final Builder builder) {
        this.version = builder.version == null ? DEFAULT_VERSION : builder.version;
        this.encodingVersion = builder.encodingVersion == null ? DEFAULT_ENCODING : builder.encodingVersion;
        this.stateValues = Collections.unmodifiableMap(new TreeMap<>(builder.stateValues));
        Objects.requireNonNull(version, "Version must be non-null");
        Objects.requireNonNull(encodingVersion, "Encoding Version must be non-null");
        Objects.requireNonNull(stateValues, "State Values must be non-null");
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public String get(String key) {
        return stateValues.get(key);
    }

    @Override
    public Map<String, String> toMap() {
        return stateValues;
    }

    public Integer getEncodingVersion() {
        return encodingVersion;
    }

    public static class Builder {

        private Long version;
        private Integer encodingVersion;
        private Map<String,String> stateValues = new TreeMap<>();

        public Builder version(final Long version) {
            this.version = version;
            return this;
        }

        public Builder encodingVersion(final Integer encodingVersion) {
            this.encodingVersion = encodingVersion;
            return this;
        }

        public Builder stateValue(final String name, String value) {
            stateValues.put(name, value);
            return this;
        }

        public Builder stateValues(final Map<String,String> stateValues) {
            this.stateValues.clear();
            if (stateValues != null) {
                this.stateValues.putAll(stateValues);
            }
            return this;
        }

        public RedisStateMap build() {
            return new RedisStateMap(this);
        }
    }

}
