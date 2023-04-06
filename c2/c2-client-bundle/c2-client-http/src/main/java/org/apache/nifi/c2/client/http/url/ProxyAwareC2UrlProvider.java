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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyAwareC2UrlProvider implements C2UrlProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyAwareC2UrlProvider.class);

    private final String c2RestApi;
    private final String c2RestPathHeartbeat;
    private final String c2RestPathAcknowledge;

    ProxyAwareC2UrlProvider(String c2RestApi, String c2RestPathHeartbeat, String c2RestPathAcknowledge) {
        this.c2RestApi = c2RestApi;
        this.c2RestPathHeartbeat = toAbsoluteUrl(c2RestPathHeartbeat)
            .orElseThrow(() -> new IllegalArgumentException("Unable to convert c2RestPathHeartbeat to absolute url. Please check C2 configuration"));
        this.c2RestPathAcknowledge = toAbsoluteUrl(c2RestPathAcknowledge)
            .orElseThrow(() -> new IllegalArgumentException("Unable to convert c2RestPathAcknowledge to absolute url. Please check C2 configuration"));
    }

    @Override
    public String getHeartbeatUrl() {
        return c2RestPathHeartbeat;
    }

    @Override
    public String getAcknowledgeUrl() {
        return c2RestPathAcknowledge;
    }

    @Override
    public Optional<String> getCallbackUrl(String absoluteUrl, String relativeUrl) {
        Optional<String> url = Optional.ofNullable(relativeUrl)
            .filter(StringUtils::isNotBlank)
            .map(this::toAbsoluteUrl)
            .filter(Optional::isPresent)
            .orElseGet(() -> Optional.ofNullable(absoluteUrl).filter(StringUtils::isNotBlank));

        if (!url.isPresent()) {
            LOG.error("Unable to provide callback url as both absoluteUrl and relativeUrl parameters were empty or null");
        }
        return url;
    }

    private Optional<String> toAbsoluteUrl(String path) {
        try {
            URL baseUrl = new URL(c2RestApi);
            URL fullUrl = new URL(baseUrl, path);
            return Optional.of(fullUrl.toString());
        } catch (MalformedURLException e) {
            LOG.error("Unable to convert restBase=" + c2RestApi + " and restPath=" + path + " to absolute url", e);
            return Optional.empty();
        }
    }
}
