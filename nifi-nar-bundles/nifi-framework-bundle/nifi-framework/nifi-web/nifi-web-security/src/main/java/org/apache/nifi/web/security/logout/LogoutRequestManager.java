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
package org.apache.nifi.web.security.logout;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.util.CacheKey;

import java.util.concurrent.TimeUnit;

public class LogoutRequestManager {

    // identifier from cookie -> logout request
    private final Cache<CacheKey, LogoutRequest> requestLookup;

    public LogoutRequestManager() {
        this(60, TimeUnit.SECONDS);
    }

    public LogoutRequestManager(final int cacheExpiration, final TimeUnit units) {
        this.requestLookup = CacheBuilder.newBuilder().expireAfterWrite(cacheExpiration, units).build();
    }

    public void start(final LogoutRequest logoutRequest) {
        if (logoutRequest == null) {
            throw new IllegalArgumentException("Logout Request is required");
        }

        final CacheKey requestIdentifierKey = new CacheKey(logoutRequest.getRequestIdentifier());

        synchronized (requestLookup) {
            final LogoutRequest existingRequest = requestLookup.getIfPresent(requestIdentifierKey);
            if (existingRequest == null) {
                requestLookup.put(requestIdentifierKey, logoutRequest);
            } else {
                throw new IllegalStateException("An existing logout request is already in progress");
            }
        }
    }

    public LogoutRequest get(final String requestIdentifier) {
        if (StringUtils.isBlank(requestIdentifier)) {
            throw new IllegalArgumentException("Request identifier is required");
        }

        final CacheKey requestIdentifierKey = new CacheKey(requestIdentifier);

        synchronized (requestLookup) {
            final LogoutRequest logoutRequest = requestLookup.getIfPresent(requestIdentifierKey);
            return logoutRequest;
        }
    }

    public LogoutRequest complete(final String requestIdentifier) {
        if (StringUtils.isBlank(requestIdentifier)) {
            throw new IllegalArgumentException("Request identifier is required");
        }

        final CacheKey requestIdentifierKey = new CacheKey(requestIdentifier);

        synchronized (requestLookup) {
            final LogoutRequest logoutRequest = requestLookup.getIfPresent(requestIdentifierKey);
            if (logoutRequest != null) {
                requestLookup.invalidate(requestIdentifierKey);
            }
            return logoutRequest;
        }
    }

}
