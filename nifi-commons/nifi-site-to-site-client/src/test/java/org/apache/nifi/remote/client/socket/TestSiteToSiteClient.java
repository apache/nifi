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
package org.apache.nifi.remote.client.socket;

import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSiteToSiteClient {

    @SuppressWarnings("deprecation")
    @Test
    public void testGetUrlBackwardCompatibility() {
        final Set<String> urls = new LinkedHashSet<>();
        urls.add("http://node1:8080/nifi");
        urls.add("http://node2:8080/nifi");
        final SiteToSiteClientConfig config = new SiteToSiteClient.Builder()
                .urls(urls)
                .buildConfig();

        assertEquals("http://node1:8080/nifi", config.getUrl());
        assertEquals(urls, config.getUrls());
    }
}
