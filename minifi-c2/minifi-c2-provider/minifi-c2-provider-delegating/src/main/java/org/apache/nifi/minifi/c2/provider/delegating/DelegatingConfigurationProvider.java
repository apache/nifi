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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.minifi.c2.api.Configuration;
import org.apache.nifi.minifi.c2.api.ConfigurationProvider;
import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.InvalidParameterException;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCache;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCacheFileInfo;
import org.apache.nifi.minifi.c2.api.cache.WriteableConfiguration;
import org.apache.nifi.minifi.c2.api.security.authorization.AuthorizationException;
import org.apache.nifi.minifi.c2.provider.util.HttpConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DelegatingConfigurationProvider implements ConfigurationProvider {
    public static final Pattern errorPattern = Pattern.compile("^Server returned HTTP response code: ([0-9]+) for URL:.*");
    private static final Logger logger = LoggerFactory.getLogger(DelegatingConfigurationProvider.class);
    private final ConfigurationCache configurationCache;
    private final HttpConnector httpConnector;
    private final ObjectMapper objectMapper;

    public DelegatingConfigurationProvider(ConfigurationCache configurationCache, String delegateUrl) throws InvalidParameterException, GeneralSecurityException, IOException {
        this(configurationCache, new HttpConnector(delegateUrl));
    }

    public DelegatingConfigurationProvider(ConfigurationCache configurationCache, HttpConnector httpConnector) {
        this.configurationCache = configurationCache;
        this.httpConnector = httpConnector;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public List<String> getContentTypes() throws ConfigurationProviderException {
        try {
            HttpURLConnection httpURLConnection = httpConnector.get("/c2/config/contentTypes");
            try {
                List<String> contentTypes = objectMapper.readValue(httpURLConnection.getInputStream(), List.class);
                if (logger.isDebugEnabled()) {
                    logger.debug("Got content types: " + contentTypes);
                }
                return contentTypes;
            } finally {
                httpURLConnection.disconnect();
            }
        } catch (IOException e) {
            throw new ConfigurationProviderException("Unable to get content types from delegate.", e);
        }
    }

    @Override
    public Configuration getConfiguration(String contentType, Integer version, Map<String, List<String>> parameters) throws ConfigurationProviderException {
        HttpURLConnection remoteC2ServerConnection = null;
        try {
            if (version == null) {
                remoteC2ServerConnection = getDelegateConnection(contentType, parameters);
                version = Integer.parseInt(remoteC2ServerConnection.getHeaderField("X-Content-Version"));
                if (logger.isDebugEnabled()) {
                    logger.debug("Got current version " + version + " from upstream.");
                }
            }
            ConfigurationCacheFileInfo cacheFileInfo = configurationCache.getCacheFileInfo(contentType, parameters);
            WriteableConfiguration configuration = cacheFileInfo.getConfiguration(version);
            if (!configuration.exists()) {
                if (remoteC2ServerConnection == null) {
                    remoteC2ServerConnection = getDelegateConnection(contentType, parameters);
                }
                try (InputStream inputStream = remoteC2ServerConnection.getInputStream();
                     OutputStream outputStream = configuration.getOutputStream()) {
                    IOUtils.copy(inputStream, outputStream);
                } catch (IOException e) {
                    throw new ConfigurationProviderException("Unable to copy remote configuration to cache.", e);
                }
            }
            return configuration;
        } finally {
            if (remoteC2ServerConnection != null) {
                remoteC2ServerConnection.disconnect();
            }
        }
    }

    protected HttpURLConnection getDelegateConnection(String contentType, Map<String, List<String>> parameters) throws ConfigurationProviderException {
        StringBuilder queryStringBuilder = new StringBuilder();
        try {
            parameters.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).forEachOrdered(e -> e.getValue().stream().sorted().forEachOrdered(v -> {
                try {
                    queryStringBuilder.append(URLEncoder.encode(e.getKey(), "UTF-8")).append("=").append(URLEncoder.encode(v, "UTF-8"));
                } catch (UnsupportedEncodingException ex) {
                    throw new ConfigurationProviderException("Unsupported encoding.", ex).wrap();
                }
                queryStringBuilder.append("&");
            }));
        } catch (ConfigurationProviderException.Wrapper e) {
            throw e.unwrap();
        }
        String url = "/c2/config";
        if (queryStringBuilder.length() > 0) {
            queryStringBuilder.setLength(queryStringBuilder.length() - 1);
            url = url + "?" + queryStringBuilder.toString();
        }
        HttpURLConnection httpURLConnection = httpConnector.get(url);
        httpURLConnection.setRequestProperty("Accepts", contentType);
        try {
            int responseCode;
            try {
                responseCode = httpURLConnection.getResponseCode();
            } catch (IOException e) {
                Matcher matcher = errorPattern.matcher(e.getMessage());
                if (matcher.matches()) {
                    responseCode = Integer.parseInt(matcher.group(1));
                } else {
                    throw e;
                }
            }
            if (responseCode >= 400) {
                String message = "";
                InputStream inputStream = httpURLConnection.getErrorStream();
                if (inputStream != null) {
                    try {
                        message = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                    } finally {
                        inputStream.close();
                    }
                }
                if (responseCode == 400) {
                    throw new InvalidParameterException(message);
                } else if (responseCode == 403) {
                    throw new AuthorizationException("Got authorization exception from upstream server " + message);
                } else {
                    throw new ConfigurationProviderException(message);
                }
            }
        } catch (IOException e) {
            throw new ConfigurationProviderException("Unable to get response code from upstream server.", e);
        }
        return httpURLConnection;
    }
}
