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

public class GenericApiGatewayRequestBuilder {
    private HttpMethodName httpMethod;
    private String resourcePath;
    private InputStream body;
    private Map<String, String> headers;
    private Map<String, List<String>> parameters;

    public GenericApiGatewayRequestBuilder withHttpMethod(HttpMethodName name) {
        httpMethod = name;
        return this;
    }

    public GenericApiGatewayRequestBuilder withResourcePath(String path) {
        resourcePath = path;
        return this;
    }

    public GenericApiGatewayRequestBuilder withBody(InputStream content) {
        this.body = content;
        return this;
    }

    public GenericApiGatewayRequestBuilder withHeaders(Map<String, String> headers) {
        this.headers = headers;
        return this;
    }

    public GenericApiGatewayRequestBuilder withParameters(Map<String,List<String>> parameters) {
        this.parameters = parameters;
        return this;
    }

    public boolean hasBody() {
        return this.body != null;
    }

    public GenericApiGatewayRequest build() {
        Validate.notNull(httpMethod, "HTTP method");
        Validate.notEmpty(resourcePath, "Resource path");
        return new GenericApiGatewayRequest(httpMethod, resourcePath, body, headers, parameters);
    }
}