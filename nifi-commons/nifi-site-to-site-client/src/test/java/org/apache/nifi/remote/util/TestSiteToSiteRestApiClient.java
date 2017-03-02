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

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

import static org.apache.nifi.remote.util.SiteToSiteRestApiClient.parseClusterUrls;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSiteToSiteRestApiClient {

    private static void assertSingleUri(final String expected, final Set<String> urls) {
        Assert.assertEquals(1, urls.size());
        Assert.assertEquals(expected, urls.iterator().next().toString());
    }

    @Test
    public void testResolveBaseUrlHttp() throws Exception{
        assertSingleUri("http://nifi.example.com/nifi-api", parseClusterUrls("http://nifi.example.com/nifi"));
    }

    @Test
    public void testResolveBaseUrlHttpSub() throws Exception{
        assertSingleUri("http://nifi.example.com/foo/bar/baz/nifi-api", parseClusterUrls("http://nifi.example.com/foo/bar/baz/nifi"));
    }

    @Test
    public void testResolveBaseUrlHttpPort() {
        assertSingleUri("http://nifi.example.com:8080/nifi-api", parseClusterUrls("http://nifi.example.com:8080/nifi"));
    }

    @Test
    public void testResolveBaseUrlHttps() throws Exception{
        assertSingleUri("https://nifi.example.com/nifi-api", parseClusterUrls("https://nifi.example.com/nifi"));
    }

    @Test
    public void testResolveBaseUrlHttpsPort() {
        assertSingleUri("https://nifi.example.com:8443/nifi-api", parseClusterUrls("https://nifi.example.com:8443/nifi"));
    }

    @Test
    public void testResolveBaseUrlLeniency() {

        String expectedUri = "http://localhost:8080/nifi-api";
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080 "));
        assertSingleUri(expectedUri, parseClusterUrls(" http://localhost:8080 "));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/nifi"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/nifi/"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/nifi/ "));
        assertSingleUri(expectedUri, parseClusterUrls(" http://localhost:8080/nifi/ "));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/nifi-api"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/nifi-api/"));

        expectedUri = "http://localhost/nifi-api";
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost/"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost/nifi"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost/nifi-api"));

        expectedUri = "http://localhost:8080/some/path/nifi-api";
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/some/path"));
        assertSingleUri(expectedUri, parseClusterUrls(" http://localhost:8080/some/path"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/some/path "));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/some/path/nifi"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/some/path/nifi/"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/some/path/nifi-api"));
        assertSingleUri(expectedUri, parseClusterUrls("http://localhost:8080/some/path/nifi-api/"));
    }

    @Test
    public void testResolveBaseUrlLeniencyHttps() {

        String expectedUri = "https://localhost:8443/nifi-api";
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443 "));
        assertSingleUri(expectedUri, parseClusterUrls(" https://localhost:8443 "));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/nifi"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/nifi/"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/nifi/ "));
        assertSingleUri(expectedUri, parseClusterUrls(" https://localhost:8443/nifi/ "));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/nifi-api"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/nifi-api/"));

        expectedUri = "https://localhost/nifi-api";
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost/"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost/nifi"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost/nifi-api"));

        expectedUri = "https://localhost:8443/some/path/nifi-api";
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/some/path"));
        assertSingleUri(expectedUri, parseClusterUrls(" https://localhost:8443/some/path"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/some/path "));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/some/path/nifi"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/some/path/nifi/"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/some/path/nifi-api"));
        assertSingleUri(expectedUri, parseClusterUrls("https://localhost:8443/some/path/nifi-api/"));
    }

    @Test
    public void testGetUrlsEmpty() throws Exception {
        try {
            parseClusterUrls(null);
            fail("Should fail if cluster URL was not specified.");
        } catch (IllegalArgumentException e) {
        }

        try {
            parseClusterUrls("");
            fail("Should fail if cluster URL was not specified.");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testGetUrlsOne() throws Exception {
        final Set<String> urls = parseClusterUrls("http://localhost:8080/nifi");

        Assert.assertEquals(1, urls.size());
        Assert.assertEquals("http://localhost:8080/nifi-api", urls.iterator().next());
    }

    @Test
    public void testGetUrlsThree() throws Exception {
        final Set<String> urls = parseClusterUrls("http://host1:8080/nifi,http://host2:8080/nifi,http://host3:8080/nifi");

        Assert.assertEquals(3, urls.size());
        final Iterator<String> iterator = urls.iterator();
        Assert.assertEquals("http://host1:8080/nifi-api", iterator.next());
        Assert.assertEquals("http://host2:8080/nifi-api", iterator.next());
        Assert.assertEquals("http://host3:8080/nifi-api", iterator.next());
    }

    @Test
    public void testGetUrlsDifferentProtocols() throws Exception {

        try {
            parseClusterUrls("http://host1:8080/nifi,https://host2:8080/nifi,http://host3:8080/nifi");
            fail("Should fail if cluster URLs contain different protocols.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Different protocols"));
        }
    }

    @Test
    public void testGetUrlsMalformed() throws Exception {

        try {
            parseClusterUrls("http://host1:8080/nifi,host&2:8080,http://host3:8080/nifi");
            fail("Should fail if cluster URLs contain illegal URL.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("malformed"));
        }
    }
}
