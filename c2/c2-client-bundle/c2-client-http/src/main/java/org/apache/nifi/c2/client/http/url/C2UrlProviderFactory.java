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

package org.apache.nifi.c2.client.http.url;

import static org.apache.commons.lang3.StringUtils.isNoneBlank;

import org.apache.nifi.c2.client.C2ClientConfig;

public class C2UrlProviderFactory {

    private static final String INCORRECT_SETTINGS_ERROR_MESSAGE = "Incorrect configuration. Please revisit C2 URL properties." +
        "Either c2.rest.url and c2.rest.url.ack have to be set," +
        "either c2.rest.path.base, c2.rest.path.heartbeat and c2.rest.path.acknowledge have to configured";

    private final C2ClientConfig clientConfig;

    public C2UrlProviderFactory(C2ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public C2UrlProvider create() {
        if (isNoneBlank(clientConfig.getC2RestPathBase(), clientConfig.getC2RestPathHeartbeat(), clientConfig.getC2RestPathAcknowledge())) {
            return new ProxyAwareC2UrlProvider(clientConfig.getC2RestPathBase(), clientConfig.getC2RestPathHeartbeat(), clientConfig.getC2RestPathAcknowledge());
        } else if (isNoneBlank(clientConfig.getC2Url(), clientConfig.getC2AckUrl())) {
            return new LegacyC2UrlProvider(clientConfig.getC2Url(), clientConfig.getC2AckUrl());
        } else {
            throw new IllegalArgumentException(INCORRECT_SETTINGS_ERROR_MESSAGE);
        }
    }
}
