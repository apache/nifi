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
package org.apache.nifi.processors.aws.wag.client;

import com.amazonaws.http.HttpMethodName;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class GenericApiGatewayRequest {

    private final HttpMethodName httpMethod;
    private final String resourcePath;
    private final InputStream body;
    private final Map<String, String> headers;
    private final Map<String, List<String>> parameters;

    public GenericApiGatewayRequest(HttpMethodName httpMethod, String resourcePath,
                                    InputStream body, Map<String, String> headers,
                                    Map<String, List<String>> parameters) {
        this.httpMethod = httpMethod;
        this.resourcePath = resourcePath;
        this.body = body;
        this.headers = headers;
        this.parameters = parameters;
    }

    public HttpMethodName getHttpMethod() {
        return httpMethod;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public InputStream getBody() {
        return body;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Map<String, List<String>> getParameters() {
        return parameters;
    }
}
