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

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Google Cloud Storage implementation of Apache Iceberg InputFile using HttpClient for REST operations
 */
class GoogleCloudStorageInputFile implements InputFile {

    private static final String QUESTION_MARK = "?";
    private static final char AMPERSAND = '&';
    private static final String FIELDS_SIZE_QUERY = "fields=size";

    private static final Pattern SIZE_PATTERN = Pattern.compile("\"size\"\\s*:\\s*\"(\\d+)\"");
    private static final int SIZE_GROUP = 1;

    private final HttpClientProvider httpClientProvider;

    private final GoogleCloudStorageProperties storageProperties;

    private final GoogleCloudStorageLocation location;

    private final String path;

    private volatile Long cachedLength;

    GoogleCloudStorageInputFile(
            final HttpClientProvider httpClientProvider,
            final GoogleCloudStorageProperties storageProperties,
            final GoogleCloudStorageLocation location,
            final String path,
            final Long cachedLength
    ) {
        this.httpClientProvider = httpClientProvider;
        this.storageProperties = storageProperties;
        this.location = location;
        this.path = path;
        this.cachedLength = cachedLength;
    }

    /**
     * Get Input File length in bytes and fetch remote object size when length is not cached
     *
     * @return Input File length in bytes
     */
    @Override
    public long getLength() {
        if (cachedLength == null) {
            cachedLength = getObjectSize();
        }
        return cachedLength;
    }

    /**
     * Create Seekable InputStream for InputFile reading
     *
     * @return Seekable InputStream
     */
    @Override
    public SeekableInputStream newStream() {
        return new GoogleCloudStorageSeekableInputStream(httpClientProvider, storageProperties, location, cachedLength);
    }

    /**
     * Get InputFile Location
     *
     * @return InputFile Location
     */
    @Override
    public String location() {
        return path;
    }

    /**
     * Get status of InputFile existence based on Metadata URI
     *
     * @return InputFile existence status
     */
    @Override
    public boolean exists() {
        final String uri = getMetadataUri();
        final HttpRequest request = httpClientProvider.newRequestBuilder(uri).GET().build();

        try {
            final HttpResponse<Void> response = httpClientProvider.send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode() == HTTP_OK;
        } catch (final IOException e) {
            throw new HttpRequestException("Metadata URI [%s] request failed".formatted(uri), e);
        }
    }

    private long getObjectSize() {
        final String uri = getMetadataUri();
        final HttpRequest request = httpClientProvider.newRequestBuilder(uri).GET().build();

        try {
            final HttpResponse<String> response = httpClientProvider.send(request, HttpResponse.BodyHandlers.ofString());
            final int statusCode = response.statusCode();
            final String responseBody = response.body();

            if (HTTP_OK == statusCode) {
                final Matcher sizeMatcher = SIZE_PATTERN.matcher(responseBody);
                if (sizeMatcher.find()) {
                    final String sizeGroup = sizeMatcher.group(SIZE_GROUP);
                    return Long.parseLong(sizeGroup);
                } else {
                    throw new HttpResponseException("Metadata URI [%s] response parsing failed: HTTP %d size not found [%s]".formatted(uri, statusCode, responseBody));
                }
            } else {
                throw new HttpResponseException("Metadata URI [%s] request failed: HTTP %d [%s]".formatted(uri, statusCode, responseBody));
            }
        } catch (final IOException e) {
            throw new HttpRequestException("Metadata URI [%s] request failed".formatted(uri), e);
        }
    }

    private String getMetadataUri() {
        final String baseUri = storageProperties.metadataUri(location);

        final StringBuilder builder = new StringBuilder(baseUri);

        if (baseUri.contains(QUESTION_MARK)) {
            builder.append(AMPERSAND);
        } else {
            builder.append(QUESTION_MARK);
        }

        builder.append(FIELDS_SIZE_QUERY);

        return builder.toString();
    }
}
