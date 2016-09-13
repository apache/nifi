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
package org.apache.nifi.remote.util;

import org.apache.nifi.events.EventReporter;
import org.junit.Assert;
import org.junit.Test;

public class TestSiteToSiteRestApiClient {

    @Test
    public void testResolveBaseUrlHttp() throws Exception{

        final SiteToSiteRestApiClient apiClient = new SiteToSiteRestApiClient(null, null, EventReporter.NO_OP);

        final String baseUrl = apiClient.resolveBaseUrl("http://nifi.example.com/nifi");
        Assert.assertEquals("http://nifi.example.com/nifi-api", baseUrl);
    }

    @Test
    public void testResolveBaseUrlHttpSub() throws Exception{

        final SiteToSiteRestApiClient apiClient = new SiteToSiteRestApiClient(null, null, EventReporter.NO_OP);

        final String baseUrl = apiClient.resolveBaseUrl("http://nifi.example.com/foo/bar/baz/nifi");
        Assert.assertEquals("http://nifi.example.com/foo/bar/baz/nifi-api", baseUrl);
    }

    @Test
    public void testResolveBaseUrlHttpPort() {
        final SiteToSiteRestApiClient apiClient = new SiteToSiteRestApiClient(null, null, EventReporter.NO_OP);

        final String baseUrl = apiClient.resolveBaseUrl("http://nifi.example.com:8080/nifi");
        Assert.assertEquals("http://nifi.example.com:8080/nifi-api", baseUrl);
    }

    @Test
    public void testResolveBaseUrlHttps() throws Exception{

        final SiteToSiteRestApiClient apiClient = new SiteToSiteRestApiClient(null, null, EventReporter.NO_OP);

        final String baseUrl = apiClient.resolveBaseUrl("https://nifi.example.com/nifi");
        Assert.assertEquals("https://nifi.example.com/nifi-api", baseUrl);
    }

    @Test
    public void testResolveBaseUrlHttpsPort() {
        final SiteToSiteRestApiClient apiClient = new SiteToSiteRestApiClient(null, null, EventReporter.NO_OP);

        final String baseUrl = apiClient.resolveBaseUrl("https://nifi.example.com:8443/nifi");
        Assert.assertEquals("https://nifi.example.com:8443/nifi-api", baseUrl);
    }

}
