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

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LegacyC2UrlProvider implements C2UrlProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LegacyC2UrlProvider.class);

    private final String c2Url;
    private final String c2AckUrl;

    LegacyC2UrlProvider(String c2Url, String c2AckUrl) {
        this.c2Url = c2Url;
        this.c2AckUrl = c2AckUrl;
    }

    @Override
    public String getHeartbeatUrl() {
        return c2Url;
    }

    @Override
    public String getAcknowledgeUrl() {
        return c2AckUrl;
    }

    @Override
    public String getCallbackUrl(String absoluteUrl, String relativeUrl) {
        return Optional.ofNullable(absoluteUrl)
                   .filter(StringUtils::isNotBlank)
                   .orElseThrow( () -> {
                      LOG.error("Provided absolute url was empty or null. Relative urls are not supported with this configuration");
                      throw new IllegalArgumentException("Provided absolute url was empty or null. Relative C2 urls are not supported with this configuration");
                   });
    }
}
