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

package org.apache.nifi.minifi.properties;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

class DuplicateDetectingProperties extends Properties {
    // Only need to retain Properties key. This will help prevent possible inadvertent exposure of sensitive Properties value
    private final Set<String> duplicateKeys = new HashSet<>();
    private final Set<String> redundantKeys = new HashSet<>();

    public DuplicateDetectingProperties() {
        super();
    }

    public Set<String> duplicateKeySet() {
        return duplicateKeys;
    }

    public Set<String> redundantKeySet() {
        return redundantKeys;
    }

    @Override
    public Object put(Object key, Object value) {
        Object existingValue = super.put(key, value);
        if (existingValue != null) {
            if (existingValue.toString().equals(value.toString())) {
                redundantKeys.add(key.toString());
            } else {
                duplicateKeys.add(key.toString());
            }
        }
        return value;
    }
}
