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
package org.apache.nifi.registry.client.impl.request;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.client.RequestConfig;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of RequestConfig for a request with basic auth.
 */
public class BasicAuthRequestConfig implements RequestConfig {

    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String BASIC = "Basic";

    private final String username;
    private final String password;

    public BasicAuthRequestConfig(final String username, final String password) {
        this.username = Validate.notBlank(username);
        this.password = Validate.notBlank(password);
    }

    @Override
    public Map<String, String> getHeaders() {
        final String basicCreds = username + ":" + password;
        final byte[] basicCredsBytes = basicCreds.getBytes(StandardCharsets.UTF_8);

        final Base64.Encoder encoder = Base64.getEncoder();
        final String encodedBasicCreds = encoder.encodeToString(basicCredsBytes);

        final Map<String,String> headers = new HashMap<>();
        headers.put(AUTHORIZATION_HEADER, BASIC + " " + encodedBasicCreds);
        return headers;
    }
}
