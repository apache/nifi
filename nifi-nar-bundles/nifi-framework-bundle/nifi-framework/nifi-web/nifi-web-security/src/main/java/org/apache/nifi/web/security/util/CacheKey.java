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
package org.apache.nifi.web.security.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * An authentication token that represents an Authenticated and Authorized user of the NiFi Apis. The authorities are based off the specified UserDetails.
 */
/**
 * Key for the cache. Necessary to override the default String.equals() to utilize MessageDigest.isEquals() to prevent timing attacks.
 */
public class CacheKey {
    final String key;

    public CacheKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CacheKey otherCacheKey = (CacheKey) o;
        return MessageDigest.isEqual(key.getBytes(StandardCharsets.UTF_8), otherCacheKey.key.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "CacheKey{token ending in '..." + key.substring(key.length() - 6) + "'}";
    }
}
