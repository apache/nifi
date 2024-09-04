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

package org.apache.nifi.cluster.coordination.http.replication.client;

import org.apache.nifi.cluster.coordination.http.replication.PreparedRequest;

import java.util.Map;

/**
 * Standard record implementation of Request prepared for Replication
 *
 * @param method HTTP Method
 * @param headers Map of HTTP Request Headers
 * @param entity HTTP Request Entity
 * @param requestBody Serialized Request Body
 */
record StandardPreparedRequest(String method, Map<String, String> headers, Object entity, byte[] requestBody) implements PreparedRequest {

    @Override
    public String getMethod() {
        return method;
    }

    @Override
    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public Object getEntity() {
        return entity;
    }
}
