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
package org.apache.nifi.services.iceberg.gcs;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serial;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.Map;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.OAUTH2_TOKEN;

/**
 * Google Cloud Storage implementation of Iceberg FileIO using Java HttpClient for REST API operations
 */
class GoogleCloudStorageFileIO implements FileIO {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(GoogleCloudStorageFileIO.class);

    private Map<String, String> properties = Collections.emptyMap();

    private transient GoogleCloudStorageProperties storageProperties;
    private transient HttpClientProvider httpClientProvider;

    /**
     * Initialize FileIO with standard properties defined in Apache Iceberg GCPProperties class
     *
     * @param properties Properties defined according to Iceberg GCPProperties
     */
    @Override
    public void initialize(final Map<String, String> properties) {
        this.properties = Map.copyOf(properties);
        this.storageProperties = new GoogleCloudStorageProperties(properties);

        final String bearerToken = properties.get(OAUTH2_TOKEN.getProperty());
        this.httpClientProvider = new HttpClientProvider(bearerToken);
    }

    /**
     * Create Iceberg Input File with unspecified length
     *
     * @param path Input File Path
     * @return Input File with unspecified length
     */
    @Override
    public InputFile newInputFile(final String path) {
        return new GoogleCloudStorageInputFile(httpClientProvider, storageProperties, GoogleCloudStorageLocation.parse(path), path, null);
    }

    /**
     * Create Iceberg Input File with length specified
     *
     * @param path Input File Path
     * @param length Input File Length in bytes
     * @return Input File with length specified
     */
    @Override
    public InputFile newInputFile(final String path, final long length) {
        return new GoogleCloudStorageInputFile(httpClientProvider, storageProperties, GoogleCloudStorageLocation.parse(path), path, length);
    }

    /**
     * Create Iceberg Output File
     *
     * @param path Output File Path
     * @return Output File
     */
    @Override
    public OutputFile newOutputFile(final String path) {
        return new GoogleCloudStorageOutputFile(httpClientProvider, storageProperties, GoogleCloudStorageLocation.parse(path), path);
    }

    /**
     * Delete File at specified location
     *
     * @param path Location of file to be deleted
     */
    @Override
    public void deleteFile(final String path) {
        final GoogleCloudStorageLocation location = GoogleCloudStorageLocation.parse(path);
        final String uri = storageProperties.metadataUri(location);
        final HttpRequest request = httpClientProvider.newRequestBuilder(uri).DELETE().build();
        try {
            final HttpResponse<String> response = httpClientProvider.send(request, HttpResponse.BodyHandlers.ofString());
            final int statusCode = response.statusCode();

            if (HTTP_NO_CONTENT == statusCode || HTTP_NOT_FOUND == statusCode) {
                logger.debug("Delete File [{}] completed: HTTP {}", path, statusCode);
            } else {
                final String responseBody = response.body();
                throw new HttpResponseException("Delete File [%s] failed: HTTP %d [%s]".formatted(path, statusCode, responseBody));
            }
        } catch (final IOException e) {
            throw new HttpRequestException("Delete File [%s] failed".formatted(path), e);
        }
    }

    /**
     * Get current configuration properties
     *
     * @return Configuration properties
     */
    @Override
    public Map<String, String> properties() {
        return properties;
    }

    /**
     * Close client resources
     */
    @Override
    public void close() {
        httpClientProvider.close();
    }
}
