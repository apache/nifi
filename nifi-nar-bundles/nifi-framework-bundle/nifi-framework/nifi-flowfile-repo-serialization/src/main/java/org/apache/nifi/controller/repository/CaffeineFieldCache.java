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

package org.apache.nifi.controller.repository;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.repository.schema.FieldCache;

public class CaffeineFieldCache implements FieldCache {
    private final Cache<String, String> cache;

    public CaffeineFieldCache(final long maxCharacters) {
        cache = Caffeine.newBuilder()
            .maximumWeight(maxCharacters)
            .weigher((k, v) -> ((String) k).length())
            .build();
    }

    @Override
    public String cache(final String value) {
        return cache.get(value, k -> value);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }
}
