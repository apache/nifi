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

import static org.apache.commons.lang3.StringUtils.appendIfMissing;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.stripStart;

import java.util.Optional;
import okhttp3.HttpUrl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyAwareC2UrlProvider implements C2UrlProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyAwareC2UrlProvider.class);
    private static final String SLASH = "/";

    private final HttpUrl c2RestPathBase;
    private final String c2RestPathHeartbeat;
    private final String c2RestPathAcknowledge;

    ProxyAwareC2UrlProvider(String c2RestPathBase, String c2RestPathHeartbeat, String c2RestPathAcknowledge) {
        this.c2RestPathBase = Optional.ofNullable(c2RestPathBase)
            .filter(StringUtils::isNotBlank)
            .map(apiBase -> appendIfMissing(apiBase, SLASH)) // trailing slash needs to be added for proper URL creation
            .map(HttpUrl::parse)
            .orElseThrow(() -> new IllegalArgumentException("Parameter c2RestPathBase should not be null or empty and should be a valid URL"));
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
    public String getCallbackUrl(String absoluteUrl, String relativeUrl) {
        return Optional.ofNullable(relativeUrl)
            .map(this::toAbsoluteUrl)
            .filter(Optional::isPresent)
            .orElseGet(() -> Optional.ofNullable(absoluteUrl).filter(StringUtils::isNotBlank))
            .orElseThrow(() -> new IllegalArgumentException("Unable to return non empty c2 url."));
    }

    private Optional<String> toAbsoluteUrl(String path) {
        if (isBlank(path)) {
            LOG.error("Unable to convert to absolute url, provided path was null or empty");
            return Optional.empty();
        }
        try {
            return Optional.of(c2RestPathBase.resolve(stripStart(path, SLASH)).toString()); // leading slash needs to be removed for proper URL creation
        } catch (Exception e) {
            LOG.error("Unable to convert restBase={} and restPath={} to absolute url", c2RestPathBase, path, e);
            return Optional.empty();
        }
    }
}
