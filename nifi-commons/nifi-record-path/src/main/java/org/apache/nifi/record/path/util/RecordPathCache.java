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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.nifi.record.path.RecordPath;

public class RecordPathCache {
    private final Map<String, RecordPath> compiledRecordPaths;

    public RecordPathCache(final int cacheSize) {
        compiledRecordPaths = new LinkedHashMap<String, RecordPath>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<String, RecordPath> eldest) {
                return size() >= cacheSize;
            }
        };
    }

    public RecordPath getCompiled(final String path) {
        RecordPath compiled;
        synchronized (this) {
            compiled = compiledRecordPaths.get(path);
        }

        if (compiled != null) {
            return compiled;
        }

        compiled = RecordPath.compile(path);

        synchronized (this) {
            final RecordPath existing = compiledRecordPaths.putIfAbsent(path, compiled);
            if (existing != null) {
                compiled = existing;
            }
        }

        return compiled;
    }
}
