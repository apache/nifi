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
import org.apache.nifi.minifi.c2.api.cache.WriteableConfiguration;
import org.apache.nifi.minifi.c2.api.util.Pair;
import org.apache.nifi.minifi.c2.provider.util.HttpConnector;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class NiFiRestConfigurationProvider implements ConfigurationProvider {
    public static final String CONTENT_TYPE = "text/yml";
    private static final Logger logger = LoggerFactory.getLogger(NiFiRestConfigurationProvider.class);
    private final JsonFactory jsonFactory = new JsonFactory();
    private final ConfigurationCache configurationCache;
    private final HttpConnector httpConnector;
    private final String templateNamePattern;

    public NiFiRestConfigurationProvider(ConfigurationCache configurationCache, String nifiUrl, String templateNamePattern) throws InvalidParameterException, GeneralSecurityException, IOException {
        this(configurationCache, new HttpConnector(nifiUrl), templateNamePattern);
    }

    public NiFiRestConfigurationProvider(ConfigurationCache configurationCache, HttpConnector httpConnector, String templateNamePattern) {
        this.configurationCache = configurationCache;
        this.httpConnector = httpConnector;
        this.templateNamePattern = templateNamePattern;
    }

    @Override
    public List<String> getContentTypes() {
        return Collections.singletonList(CONTENT_TYPE);
    }

    @Override
    public Configuration getConfiguration(String contentType, Integer version, Map<String, List<String>> parameters) throws ConfigurationProviderException {
        if (!CONTENT_TYPE.equals(contentType)) {
            throw new ConfigurationProviderException("Unsupported content type: " + contentType + " supported value is " + CONTENT_TYPE);
        }
        String filename = templateNamePattern;
        for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
            if (entry.getValue().size() != 1) {
                throw new InvalidParameterException("Multiple values for same parameter not supported in this provider.");
            }
            filename = filename.replaceAll(Pattern.quote("${" + entry.getKey() + "}"), entry.getValue().get(0));
        }
        int index = filename.indexOf("${");
        while (index != -1) {
            int endIndex = filename.indexOf("}", index);
            if (endIndex == -1) {
                break;
            }
            String variable = filename.substring(index + 2, endIndex);
            if (!"version".equals(variable)) {
                throw new InvalidParameterException("Found unsubstituted parameter " + variable);
            }
            index = endIndex + 1;
        }

        String id = null;
        if (version == null) {
            String filenamePattern = Arrays.stream(filename.split(Pattern.quote("${version}"), -1)).map(Pattern::quote).collect(Collectors.joining("([0-9+])"));
            Pair<String, Integer> maxIdAndVersion = getMaxIdAndVersion(filenamePattern);
            id = maxIdAndVersion.getFirst();
            version = maxIdAndVersion.getSecond();
        }
        filename = filename.replaceAll(Pattern.quote("${version}"), Integer.toString(version));
        WriteableConfiguration configuration = configurationCache.getCacheFileInfo(contentType, parameters).getConfiguration(version);
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
                    String tmpFilename = templateNamePattern;
                    for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
                        if (entry.getValue().size() != 1) {
                            throw new InvalidParameterException("Multiple values for same parameter not supported in this provider.");
                        }
                        tmpFilename = tmpFilename.replaceAll(Pattern.quote("${" + entry.getKey() + "}"), entry.getValue().get(0));
                    }
                    Pair<Stream<Pair<String, String>>, Closeable> streamCloseablePair = getIdAndFilenameStream();
                    try {
                        String finalFilename = filename;
                        id = streamCloseablePair.getFirst().filter(p -> finalFilename.equals(p.getSecond())).map(Pair::getFirst).findFirst()
                                .orElseThrow(() -> new InvalidParameterException("Unable to find template named " + finalFilename));
                    } finally {
                        streamCloseablePair.getSecond().close();
                    }
                } catch (IOException|TemplatesIteratorException e) {
                    throw new ConfigurationProviderException("Unable to retrieve template list", e);
                }
            }

            HttpURLConnection urlConnection = httpConnector.get("/templates/" + id + "/download");

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
        TemplatesIterator templatesIterator = new TemplatesIterator(httpConnector, jsonFactory);
        return new Pair<>(StreamSupport.stream(Spliterators.spliteratorUnknownSize(templatesIterator, Spliterator.ORDERED), false), templatesIterator);
    }

    private Pair<Stream<Pair<String, Integer>>, Closeable> getIdAndVersionStream(String filenamePattern) throws ConfigurationProviderException, IOException {
        Pattern filename = Pattern.compile(filenamePattern);
        Pair<Stream<Pair<String, String>>, Closeable> streamCloseablePair = getIdAndFilenameStream();
        return new Pair<>(streamCloseablePair.getFirst().map(p -> {
            Matcher matcher = filename.matcher(p.getSecond());
            if (!matcher.matches()) {
                return null;
            }
            return new Pair<>(p.getFirst(), Integer.parseInt(matcher.group(1)));
        }).filter(Objects::nonNull), streamCloseablePair.getSecond());
    }

    private Pair<String, Integer> getMaxIdAndVersion(String filenamePattern) throws ConfigurationProviderException {
        try {
            Pair<Stream<Pair<String, Integer>>, Closeable> streamCloseablePair = getIdAndVersionStream(filenamePattern);
            try {
                return streamCloseablePair.getFirst().sorted(Comparator.comparing(p -> ((Pair<String, Integer>) p).getSecond()).reversed()).findFirst()
                        .orElseThrow(() -> new ConfigurationProviderException("Didn't find any templates that matched " + filenamePattern));
            } finally {
                streamCloseablePair.getSecond().close();
            }
        } catch (IOException|TemplatesIteratorException e) {
            throw new ConfigurationProviderException("Unable to retrieve template list", e);
        }
    }
}
