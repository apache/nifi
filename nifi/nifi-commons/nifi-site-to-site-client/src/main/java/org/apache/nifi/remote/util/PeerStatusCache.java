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
package org.apache.nifi.remote.util;

import java.util.Set;

import org.apache.nifi.remote.PeerStatus;

public class PeerStatusCache {
	private final Set<PeerStatus> statuses;
    private final long timestamp;

    public PeerStatusCache(final Set<PeerStatus> statuses) {
        this(statuses, System.currentTimeMillis());
    }

    public PeerStatusCache(final Set<PeerStatus> statuses, final long timestamp) {
        this.statuses = statuses;
        this.timestamp = timestamp;
    }

    public Set<PeerStatus> getStatuses() {
        return statuses;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
