/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.elasticsearch;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapBuilder {
    private final Map<String, Object> toBuild;

    public MapBuilder() {
        toBuild = new LinkedHashMap<>();
    }

    public MapBuilder of(final String key, final Object value) {
        toBuild.put(key, value);
        return this;
    }

    public MapBuilder of(final String key, final Object value, final String key2, final Object value2) {
        toBuild.put(key, value);
        toBuild.put(key2, value2);
        return this;
    }

    public MapBuilder of(final String key, final Object value, final String key2, final Object value2, final String key3, final Object value3) {
        toBuild.put(key, value);
        toBuild.put(key2, value2);
        toBuild.put(key3, value3);
        return this;
    }

    public MapBuilder of(final String key, final Object value, final String key2, final Object value2, final String key3, final Object value3,
                         final String key4, final Object value4) {
        toBuild.put(key, value);
        toBuild.put(key2, value2);
        toBuild.put(key3, value3);
        toBuild.put(key4, value4);
        return this;
    }

    public MapBuilder of(final String key, final Object value, final String key2, final Object value2, final String key3, final Object value3,
                         final String key4, final Object value4, final String key5, final Object value5) {
        toBuild.put(key, value);
        toBuild.put(key2, value2);
        toBuild.put(key3, value3);
        toBuild.put(key4, value4);
        toBuild.put(key5, value5);
        return this;
    }

    public Map<String, Object> build() {
        return Collections.unmodifiableMap(toBuild);
    }
}
