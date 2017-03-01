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

package org.apache.nifi.minifi.c2.provider.cache;

import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCache;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCacheFileInfo;
import org.apache.nifi.minifi.c2.api.cache.WriteableConfiguration;
import org.apache.nifi.minifi.c2.provider.cache.CacheConfigurationProvider;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheConfigurationProviderTest {
    public static final String TEST_CONTENT_TYPE = "test/contenttype";

    private CacheConfigurationProvider cacheConfigurationProvider;
    private ConfigurationCache configConfigurationCache;

    @Before
    public void setup() {
        configConfigurationCache = mock(ConfigurationCache.class);
        cacheConfigurationProvider = new CacheConfigurationProvider(TEST_CONTENT_TYPE, configConfigurationCache);
    }

    @Test
    public void testContentType() {
        assertEquals(TEST_CONTENT_TYPE, cacheConfigurationProvider.getContentType());
    }

    @Test
    public void testGetConfiguration() throws ConfigurationProviderException {
        int version = 99;

        Map<String, List<String>> parameters = mock(Map.class);
        ConfigurationCacheFileInfo configurationCacheFileInfo = mock(ConfigurationCacheFileInfo.class);
        WriteableConfiguration configuration = mock(WriteableConfiguration.class);

        when(configConfigurationCache.getCacheFileInfo(parameters)).thenReturn(configurationCacheFileInfo);
        when(configurationCacheFileInfo.getConfiguration(version)).thenReturn(configuration);

        assertEquals(configuration, cacheConfigurationProvider.getConfiguration(version, parameters));
    }
}
