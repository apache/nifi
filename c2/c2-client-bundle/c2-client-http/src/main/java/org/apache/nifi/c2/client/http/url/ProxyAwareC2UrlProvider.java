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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.net.MalformedURLException;
import java.net.URL;

public class ProxyAwareC2UrlProvider implements C2UrlProvider {

    private final String c2RestApi;
    private final String c2RestPathHeartbeat;
    private final String c2RestPathAcknowledge;

    ProxyAwareC2UrlProvider(String c2RestApi, String c2RestPathHeartbeat, String c2RestPathAcknowledge) {
        this.c2RestApi = c2RestApi;
        this.c2RestPathHeartbeat = c2RestPathHeartbeat;
        this.c2RestPathAcknowledge = c2RestPathAcknowledge;
    }

    @Override
    public String getHeartbeatUrl() {
        return toAbsoluteUrl(c2RestPathHeartbeat);
    }

    @Override
    public String getAcknowledgeUrl() {
        return toAbsoluteUrl(c2RestPathAcknowledge);
    }

    @Override
    public String getCallbackUrl(String absoluteUrl, String relativeUrl) throws Exception {
        if (isNotBlank(relativeUrl)) {
            return toAbsoluteUrl(relativeUrl);
        }
        if (isNotBlank(absoluteUrl)) {
            return absoluteUrl;
        }
        throw new Exception("Unable to provide callback url as both parameters were empty or null");
    }

    private String toAbsoluteUrl(String path) {
        try {
            URL baseUrl = new URL(c2RestApi);
            URL fullUrl = new URL(baseUrl, path);
            return fullUrl.toString();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
