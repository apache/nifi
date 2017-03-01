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

import com.fasterxml.jackson.core.JsonFactory;
import org.apache.nifi.minifi.c2.api.Configuration;
import org.apache.nifi.minifi.c2.api.ConfigurationProvider;
import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.InvalidParameterException;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCache;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCacheFileInfo;
import org.apache.nifi.minifi.c2.api.cache.WriteableConfiguration;
import org.apache.nifi.minifi.c2.api.util.Pair;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaSaver;
import org.apache.nifi.minifi.toolkit.configuration.ConfigMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.GeneralSecurityException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class NiFiRestConfigurationProvider implements ConfigurationProvider {
    public static final String CONTENT_TYPE = "text/yml";
    private static final Logger logger = LoggerFactory.getLogger(NiFiRestConfigurationProvider.class);
    private final JsonFactory jsonFactory = new JsonFactory();
    private final ConfigurationCache configurationCache;
    private final NiFiRestConnector niFiRestConnector;

    public NiFiRestConfigurationProvider(ConfigurationCache configurationCache, String nifiUrl) throws InvalidParameterException, GeneralSecurityException, IOException {
        this(configurationCache, new NiFiRestConnector(nifiUrl));
    }

    public NiFiRestConfigurationProvider(ConfigurationCache configurationCache, NiFiRestConnector niFiRestConnector) {
        this.configurationCache = configurationCache;
        this.niFiRestConnector = niFiRestConnector;
    }

    @Override
    public String getContentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Configuration getConfiguration(Integer version, Map<String, List<String>> parameters) throws ConfigurationProviderException {
        ConfigurationCacheFileInfo configurationCacheFileInfo = configurationCache.getCacheFileInfo(parameters);
        String id = null;
        if (version == null) {
            Pair<String, Integer> maxIdAndVersion = getMaxIdAndVersion(configurationCacheFileInfo);
            id = maxIdAndVersion.getFirst();
            version = maxIdAndVersion.getSecond();
        }
        WriteableConfiguration configuration = configurationCacheFileInfo.getConfiguration(version);
        if (configuration.exists()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Configuration " + configuration + " exists and can be served from configurationCache.");
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Configuration " + configuration + " doesn't exist, will need to download and convert template.");
            }
            if (id == null) {
                try {
                    String filename = configuration.getName();
                    Pair<Stream<Pair<String, String>>, Closeable> streamCloseablePair = getIdAndFilenameStream();
                    try {
                        id = streamCloseablePair.getFirst().filter(p -> filename.equals(p.getSecond())).map(Pair::getFirst).findFirst()
                                .orElseThrow(() -> new InvalidParameterException("Unable to find template named " + filename));
                    } finally {
                        streamCloseablePair.getSecond().close();
                    }
                } catch (IOException|TemplatesIteratorException e) {
                    throw new ConfigurationProviderException("Unable to retrieve template list", e);
                }
            }

            HttpURLConnection urlConnection = niFiRestConnector.get("/templates/" + id + "/download");

            try (InputStream inputStream = urlConnection.getInputStream()){
                ConfigSchema configSchema = ConfigMain.transformTemplateToSchema(inputStream);
                SchemaSaver.saveConfigSchema(configSchema, configuration.getOutputStream());
            } catch (IOException e) {
                throw new ConfigurationProviderException("Unable to download template from url " + urlConnection.getURL(), e);
            } catch (JAXBException e) {
                throw new ConfigurationProviderException("Unable to convert template to yaml", e);
            } finally {
                urlConnection.disconnect();
            }
        }
        return configuration;
    }

    private Pair<Stream<Pair<String, String>>, Closeable> getIdAndFilenameStream() throws ConfigurationProviderException, IOException {
        TemplatesIterator templatesIterator = new TemplatesIterator(niFiRestConnector, jsonFactory);
        return new Pair<>(StreamSupport.stream(Spliterators.spliteratorUnknownSize(templatesIterator, Spliterator.ORDERED), false), templatesIterator);
    }

    private Pair<Stream<Pair<String, Integer>>, Closeable> getIdAndVersionStream(ConfigurationCacheFileInfo configurationCacheFileInfo) throws ConfigurationProviderException, IOException {
        Pair<Stream<Pair<String, String>>, Closeable> streamCloseablePair = getIdAndFilenameStream();
        return new Pair<>(streamCloseablePair.getFirst().map(p -> {
            Integer version = configurationCacheFileInfo.getVersionIfMatch(p.getSecond());
            if (version == null) {
                return null;
            }
            return new Pair<>(p.getFirst(), version);
        }).filter(Objects::nonNull), streamCloseablePair.getSecond());
    }

    private Pair<String, Integer> getMaxIdAndVersion(ConfigurationCacheFileInfo configurationCacheFileInfo) throws ConfigurationProviderException {
        try {
            Pair<Stream<Pair<String, Integer>>, Closeable> streamCloseablePair = getIdAndVersionStream(configurationCacheFileInfo);
            try {
                return streamCloseablePair.getFirst().sorted(Comparator.comparing(p -> ((Pair<String, Integer>) p).getSecond()).reversed()).findFirst()
                        .orElseThrow(() -> new ConfigurationProviderException("Didn't find any templates that matched " + configurationCacheFileInfo + ".v[0-9]+"));
            } finally {
                streamCloseablePair.getSecond().close();
            }
        } catch (IOException|TemplatesIteratorException e) {
            throw new ConfigurationProviderException("Unable to retrieve template list", e);
        }
    }
}
