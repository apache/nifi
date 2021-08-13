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

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Set;

import static org.apache.nifi.remote.util.SiteToSiteRestApiClient.parseClusterUrls;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSiteToSiteRestApiClient {

    private static void assertSingleUri(final String expected, final Set<String> urls) {
        assertEquals(1, urls.size());
        assertEquals(expected, urls.iterator().next().toString());
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
        assertThrows(IllegalArgumentException.class, () -> parseClusterUrls(null));

        assertThrows(IllegalArgumentException.class, () -> parseClusterUrls(""));
    }

    @Test
    public void testGetUrlsOne() throws Exception {
        final Set<String> urls = parseClusterUrls("http://localhost:8080/nifi");

        assertEquals(1, urls.size());
        assertEquals("http://localhost:8080/nifi-api", urls.iterator().next());
    }

    @Test
    public void testGetUrlsThree() throws Exception {
        final Set<String> urls = parseClusterUrls("http://host1:8080/nifi,http://host2:8080/nifi,http://host3:8080/nifi");

        assertEquals(3, urls.size());
        final Iterator<String> iterator = urls.iterator();
        assertEquals("http://host1:8080/nifi-api", iterator.next());
        assertEquals("http://host2:8080/nifi-api", iterator.next());
        assertEquals("http://host3:8080/nifi-api", iterator.next());
    }

    @Test
    public void testGetUrlsDifferentProtocols() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> parseClusterUrls("http://host1:8080/nifi,https://host2:8080/nifi,http://host3:8080/nifi"));

        assertTrue(exception.getMessage().contains("Different protocols"));
    }

    @Test
    public void testGetUrlsMalformed() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> parseClusterUrls("http://host1:8080/nifi,host&2:8080,http://host3:8080/nifi"));
        assertTrue(exception.getMessage().contains("malformed"));
    }
}
