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
package org.apache.nifi.security.util;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * This enum is used to indicate the three possible options for a server requesting a client certificate during TLS handshake negotiation.
 */
public enum ClientAuth {
    WANT("Want", "Requests the client certificate on handshake and validates if present but does not require it"),
    REQUIRED("Required", "Requests the client certificate on handshake and rejects the connection if it is not present and valid"),
    NONE("None", "Does not request the client certificate on handshake");

    private final String type;
    private final String description;

    ClientAuth(String type, String description) {
        this.type = type;
        this.description = description;
    }

    public String getType() {
        return this.type;
    }

    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[SslContextFactory]");
        sb.append("type=").append(type);
        sb.append("description=").append(description);
        return sb.toString();
    }

    /**
     * Returns {@code true} if the provided type is a valid {@link ClientAuth} type.
     *
     * @param type the raw type string
     * @return true if the type is valid
     */
    public static boolean isValidClientAuthType(String type) {
        if (type == null || type.replaceAll("\\s", "").isEmpty()) {
            return false;
        }
        return (Arrays.stream(values()).map(ca -> ca.getType().toLowerCase()).collect(Collectors.toList()).contains(type.toLowerCase()));
    }
}
