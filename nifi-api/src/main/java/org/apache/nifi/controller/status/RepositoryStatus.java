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
package org.apache.nifi.controller.status;

/**
 * The status of a repository.
 */
public class RepositoryStatus {
    public enum Repository {FLOWFILE, CONTENT, PROVENANCE}

    private final Repository type;
    private final String identifier;
    private final long maxBytes;
    private final long usedBytes;

    public RepositoryStatus(Repository type, String identifier, long maxBytes, long usedBytes) {
        this.type = type;
        this.identifier = identifier;
        this.maxBytes = maxBytes;
        this.usedBytes = usedBytes;
    }

    public Repository getType() {
        return type;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public long getUsedBytes() {
        return usedBytes;
    }

    public String getIdentifier() {
        return identifier;
    }

}

