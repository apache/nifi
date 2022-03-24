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

package org.apache.nifi.record.path.util;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.nifi.record.path.RecordPath;

public class RecordPathCache {
    private final LoadingCache<String, RecordPath> compiledRecordPaths;

    public RecordPathCache(final int cacheSize) {
        compiledRecordPaths = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .build(RecordPath::compile);
    }

    public RecordPath getCompiled(final String path) {
        return compiledRecordPaths.get(path);
    }
}
