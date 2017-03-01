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

package org.apache.nifi.minifi.c2.provider.nifi.rest;

import org.apache.nifi.minifi.c2.api.cache.ConfigurationCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class NiFiRestConfigurationProviderTest {
    private ConfigurationCache configConfigurationCache;
    private NiFiRestConnector niFiRestConnector;
    private NiFiRestConfigurationProvider niFiRestConfigurationProvider;
    private Path cachePath;

    @Before
    public void setup() throws IOException {
        configConfigurationCache = mock(ConfigurationCache.class);
        niFiRestConnector = mock(NiFiRestConnector.class);
        niFiRestConfigurationProvider = new NiFiRestConfigurationProvider(configConfigurationCache, niFiRestConnector);
        cachePath = Files.createTempDirectory(NiFiRestConfigurationProviderTest.class.getCanonicalName());
    }

    @After
    public void teardown() throws IOException {
        Files.walk(cachePath)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        p.toFile().deleteOnExit();
                    }
                });
    }

    @Test
    public void testContentType() {
        assertEquals(NiFiRestConfigurationProvider.CONTENT_TYPE, niFiRestConfigurationProvider.getContentType());
    }


}
