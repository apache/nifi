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

package org.apache.nifi.minifi.c2.provider.delegating;

import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.InvalidParameterException;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCache;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCacheFileInfo;
import org.apache.nifi.minifi.c2.api.cache.WriteableConfiguration;
import org.apache.nifi.minifi.c2.api.security.authorization.AuthorizationException;
import org.apache.nifi.minifi.c2.provider.util.HttpConnector;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DelegatingConfigurationProviderTest {
    private ConfigurationCache configurationCache;
    private HttpConnector httpConnector;
    private HttpURLConnection httpURLConnection;
    private DelegatingConfigurationProvider delegatingConfigurationProvider;
    private Map<String, List<String>> parameters;
    private Integer version;
    private String endpointPath;
    private String contentType;

    @Before
    public void setup() throws ConfigurationProviderException {
        contentType = "text/yml";
        version = 2;
        parameters = new HashMap<>();
        parameters.put("net", Collections.singletonList("edge"));
        parameters.put("class", Collections.singletonList("raspi3"));
        parameters.put("version", Collections.singletonList(Integer.toString(version)));
        endpointPath = "/c2/config?class=raspi3&net=edge&version=2";
        initMocks();
    }

    public void initMocks() throws ConfigurationProviderException {
        configurationCache = mock(ConfigurationCache.class);
        httpConnector = mock(HttpConnector.class);
        httpURLConnection = mock(HttpURLConnection.class);
        delegatingConfigurationProvider = new DelegatingConfigurationProvider(configurationCache, httpConnector);
        when(httpConnector.get(endpointPath)).thenReturn(httpURLConnection);
    }

    @Test
    public void testGetDelegateConnectionNoParameters() throws ConfigurationProviderException {
        endpointPath = "/c2/config";
        initMocks();
        assertEquals(httpURLConnection, delegatingConfigurationProvider.getDelegateConnection(contentType, Collections.emptyMap()));
        verify(httpURLConnection).setRequestProperty("Accepts", contentType);
    }

    @Test
    public void testGetDelegateConnectionParameters() throws ConfigurationProviderException {
        assertEquals(httpURLConnection, delegatingConfigurationProvider.getDelegateConnection(contentType, parameters));
        verify(httpURLConnection).setRequestProperty("Accepts", contentType);
    }

    @Test(expected = AuthorizationException.class)
    public void testGetDelegateConnection403() throws ConfigurationProviderException, IOException {
        when(httpURLConnection.getResponseCode()).thenReturn(403);
        delegatingConfigurationProvider.getDelegateConnection(contentType, parameters);
    }

    @Test(expected = InvalidParameterException.class)
    public void testGetDelegateConnection400() throws IOException, ConfigurationProviderException {
        when(httpURLConnection.getResponseCode()).thenThrow(new IOException("Server returned HTTP response code: 400 for URL: " + endpointPath));
        delegatingConfigurationProvider.getDelegateConnection(contentType, parameters);
    }

    @Test(expected = ConfigurationProviderException.class)
    public void testGetDelegateConnection401() throws IOException, ConfigurationProviderException {
        when(httpURLConnection.getResponseCode()).thenReturn(401);
        delegatingConfigurationProvider.getDelegateConnection(contentType, parameters);
    }

    @Test(expected = ConfigurationProviderException.class)
    public void testGetContentTypesMalformed() throws ConfigurationProviderException, IOException {
        endpointPath = "/c2/config/contentTypes";
        initMocks();
        when(httpURLConnection.getInputStream()).thenReturn(new ByteArrayInputStream("[malformed".getBytes(StandardCharsets.UTF_8)));
        delegatingConfigurationProvider.getContentTypes();
    }

    @Test(expected = ConfigurationProviderException.class)
    public void testGetContentTypesIOE() throws ConfigurationProviderException, IOException {
        endpointPath = "/c2/config/contentTypes";
        initMocks();
        when(httpURLConnection.getInputStream()).thenThrow(new IOException());
        delegatingConfigurationProvider.getContentTypes();
    }

    @Test
    public void testGetContentTypes() throws ConfigurationProviderException, IOException {
        endpointPath = "/c2/config/contentTypes";
        initMocks();
        List<String> contentTypes = Arrays.asList(contentType, "application/json");
        when(httpURLConnection.getInputStream()).thenReturn(new ByteArrayInputStream(("[\"" + String.join("\", \"", contentTypes) + "\"]").getBytes(StandardCharsets.UTF_8)));
        assertEquals(contentTypes, delegatingConfigurationProvider.getContentTypes());
    }

    @Test
    public void testGetConfigurationExistsWithVersion() throws ConfigurationProviderException {
        ConfigurationCacheFileInfo configurationCacheFileInfo = mock(ConfigurationCacheFileInfo.class);
        WriteableConfiguration configuration = mock(WriteableConfiguration.class);
        when(configurationCache.getCacheFileInfo(contentType, parameters)).thenReturn(configurationCacheFileInfo);
        when(configurationCacheFileInfo.getConfiguration(version)).thenReturn(configuration);
        when(configuration.exists()).thenReturn(true);

        assertEquals(configuration, delegatingConfigurationProvider.getConfiguration(contentType, version, parameters));
    }

    @Test
    public void testGetConfigurationDoesntExistWithVersion() throws ConfigurationProviderException, IOException {
        ConfigurationCacheFileInfo configurationCacheFileInfo = mock(ConfigurationCacheFileInfo.class);
        WriteableConfiguration configuration = mock(WriteableConfiguration.class);
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        when(httpURLConnection.getInputStream()).thenReturn(new ByteArrayInputStream(payload));
        when(configuration.getOutputStream()).thenReturn(output);
        when(configurationCache.getCacheFileInfo(contentType, parameters)).thenReturn(configurationCacheFileInfo);
        when(configurationCacheFileInfo.getConfiguration(version)).thenReturn(configuration);
        when(configuration.exists()).thenReturn(false);

        assertEquals(configuration, delegatingConfigurationProvider.getConfiguration(contentType, version, parameters));
        assertArrayEquals(payload, output.toByteArray());
    }

    @Test
    public void testGetConfigurationExistsWithNoVersion() throws ConfigurationProviderException {
        parameters.remove("version");
        endpointPath = "/c2/config?class=raspi3&net=edge";
        initMocks();
        ConfigurationCacheFileInfo configurationCacheFileInfo = mock(ConfigurationCacheFileInfo.class);
        WriteableConfiguration configuration = mock(WriteableConfiguration.class);
        when(httpURLConnection.getHeaderField("X-Content-Version")).thenReturn("2");
        when(configurationCache.getCacheFileInfo(contentType, parameters)).thenReturn(configurationCacheFileInfo);
        when(configurationCacheFileInfo.getConfiguration(version)).thenReturn(configuration);
        when(configuration.exists()).thenReturn(true);

        assertEquals(configuration, delegatingConfigurationProvider.getConfiguration(contentType, null, parameters));
    }

    @Test
    public void testGetConfigurationDoesntExistWithNoVersion() throws ConfigurationProviderException, IOException {
        parameters.remove("version");
        endpointPath = "/c2/config?class=raspi3&net=edge";
        initMocks();
        ConfigurationCacheFileInfo configurationCacheFileInfo = mock(ConfigurationCacheFileInfo.class);
        WriteableConfiguration configuration = mock(WriteableConfiguration.class);
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        when(httpURLConnection.getInputStream()).thenReturn(new ByteArrayInputStream(payload));
        when(configuration.getOutputStream()).thenReturn(output);
        when(httpURLConnection.getHeaderField("X-Content-Version")).thenReturn("2");
        when(configurationCache.getCacheFileInfo(contentType, parameters)).thenReturn(configurationCacheFileInfo);
        when(configurationCacheFileInfo.getConfiguration(version)).thenReturn(configuration);
        when(configuration.exists()).thenReturn(false);

        assertEquals(configuration, delegatingConfigurationProvider.getConfiguration(contentType, null, parameters));
        assertArrayEquals(payload, output.toByteArray());
    }
}
