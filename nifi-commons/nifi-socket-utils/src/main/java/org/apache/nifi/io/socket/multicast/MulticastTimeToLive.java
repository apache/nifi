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
package org.apache.nifi.io.socket.multicast;

/**
 */
public enum MulticastTimeToLive {

    SAME_HOST(0),
    SAME_SUBNET(1),
    SAME_SITE(32),
    SAME_REGION(64),
    SAME_CONTINENT(128),
    UNRESTRICTED(255);

    private final int ttl;

    MulticastTimeToLive(final int ttl) {
        this.ttl = ttl;
    }

    public int getTtl() {
        return ttl;
    }

    public MulticastTimeToLive valueOfByTtl(final int ttl) {
        for (final MulticastTimeToLive value : values()) {
            if (value.getTtl() == ttl) {
                return value;
            }
        }
        return null;
    }

}
