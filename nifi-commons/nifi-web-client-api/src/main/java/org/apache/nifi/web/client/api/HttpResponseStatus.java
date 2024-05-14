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
package org.apache.nifi.web.client.api;

/**
 * Enumeration of Standard HTTP Response Status Codes
 */
public enum HttpResponseStatus {
    OK(200),

    CREATED(201),

    ACCEPTED(202),

    NO_CONTENT(204),

    MOVED_PERMANENTLY(301),

    BAD_REQUEST(400),

    UNAUTHORIZED(401),

    FORBIDDEN(403),

    NOT_FOUND(404),

    METHOD_NOT_ALLOWED(405),

    PROXY_AUTHENTICATION_REQUIRED(407),

    CONFLICT(409),

    INTERNAL_SERVER_ERROR(500),

    SERVICE_UNAVAILABLE(503);

    private final int code;

    HttpResponseStatus(final int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

}
