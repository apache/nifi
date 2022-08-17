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
package org.apache.nifi.registry.security.authorization;

import java.util.StringJoiner;

/**
 * Actions a user/entity can take on a resource.
 */
public enum RequestAction {
    READ("read"),
    WRITE("write"),
    DELETE("delete");

    private String value;

    RequestAction(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value.toLowerCase();
    }

    public static RequestAction valueOfValue(final String action) {
        if (RequestAction.READ.toString().equalsIgnoreCase(action)) {
            return RequestAction.READ;
        } else if (RequestAction.WRITE.toString().equalsIgnoreCase(action)) {
            return RequestAction.WRITE;
        } else if (RequestAction.DELETE.toString().equalsIgnoreCase(action)) {
            return RequestAction.DELETE;
        } else {
            StringJoiner stringJoiner = new StringJoiner(", ");
            for(RequestAction ra : RequestAction.values()) {
                stringJoiner.add(ra.toString());
            }
            String allowableValues = stringJoiner.toString();
            throw new IllegalArgumentException("Action '" + action + "' is invalid. Must be one of [" + allowableValues + "]");
        }
    }
}
