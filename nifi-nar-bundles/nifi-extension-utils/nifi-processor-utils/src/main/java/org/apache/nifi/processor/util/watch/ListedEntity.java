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

package org.apache.nifi.processor.util.watch;

import java.util.Collections;
import java.util.Map;

/**
 * TODO: doc
 */
public class ListedEntity {

    /**
     * Milliseconds.
     */
    private final long lastModifiedTimestamp;
    /**
     * Bytes.
     */
    private final long size;

    public ListedEntity(long lastModifiedTimestamp, long size) {
        this.lastModifiedTimestamp = lastModifiedTimestamp;
        this.size = size;
    }

    public long getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    public long getSize() {
        return size;
    }

    public Map<String, String> createAttributes() {
        return Collections.emptyMap();
    }

}
