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
package org.apache.nifi.processors.asana.utils;

import org.apache.nifi.util.StringUtils;

import java.util.Optional;

public class AsanaObject {
    private final AsanaObjectState state;
    private final String gid;
    private final String content;
    private final String fingerprint;

    public AsanaObject(AsanaObjectState state, String gid) {
        this(state, gid, StringUtils.EMPTY);
    }

    public AsanaObject(AsanaObjectState state, String gid, String content) {
        this(state, gid, content, null);
    }

    public AsanaObject(AsanaObjectState state, String gid, String content, String fingerprint) {
        this.state = state;
        this.gid = gid;
        this.content = content;
        this.fingerprint = fingerprint;
    }

    public AsanaObjectState getState() {
        return state;
    }

    public String getGid() {
        return gid;
    }

    public String getContent() {
        return content;
    }

    public String getFingerprint() {
        return Optional.ofNullable(fingerprint).orElse(content);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AsanaObject other) {
            return state.equals(other.state)
                && Optional.ofNullable(gid).equals(Optional.ofNullable(other.gid))
                && Optional.ofNullable(content).equals(Optional.ofNullable(other.content))
                && Optional.ofNullable(fingerprint).equals(Optional.ofNullable(other.fingerprint));
        } else {
            return false;
        }
    }
}
