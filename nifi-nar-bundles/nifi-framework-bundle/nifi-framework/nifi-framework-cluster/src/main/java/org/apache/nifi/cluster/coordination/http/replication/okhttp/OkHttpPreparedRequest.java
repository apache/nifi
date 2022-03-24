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

package org.apache.nifi.cluster.coordination.http.replication.okhttp;

import java.util.Map;

import org.apache.nifi.cluster.coordination.http.replication.PreparedRequest;

import okhttp3.RequestBody;

public class OkHttpPreparedRequest implements PreparedRequest {
    private final String method;
    private final Map<String, String> headers;
    private final Object entity;
    private final RequestBody requestBody;

    public OkHttpPreparedRequest(final String method, final Map<String, String> headers, final Object entity, final RequestBody requestBody) {
        this.method = method;
        this.headers = headers;
        this.entity = entity;
        this.requestBody = requestBody;
    }

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

    public RequestBody getRequestBody() {
        return requestBody;
    }

    @Override
    public String toString() {
        return "OkHttpPreparedRequest[method=" + method + ", headers=" + headers + "]";
    }
}
