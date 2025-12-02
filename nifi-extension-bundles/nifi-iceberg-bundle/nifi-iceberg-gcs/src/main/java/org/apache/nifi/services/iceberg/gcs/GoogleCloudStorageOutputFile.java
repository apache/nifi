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

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Google Cloud Storage implementation of Apache Iceberg OutputFile using HttpClient for REST Operations
 */
class GoogleCloudStorageOutputFile implements OutputFile {

    private final HttpClientProvider httpClientProvider;

    private final GoogleCloudStorageProperties storageProperties;

    private final GoogleCloudStorageLocation location;

    private final String path;

    GoogleCloudStorageOutputFile(
            final HttpClientProvider httpClientProvider,
            final GoogleCloudStorageProperties storageProperties,
            final GoogleCloudStorageLocation location,
            final String path
    ) {
        this.httpClientProvider = httpClientProvider;
        this.storageProperties = storageProperties;
        this.location = location;
        this.path = path;
    }

    /**
     * Create OutputStream after checking for existing InputFile to avoid potential conflicts
     *
     * @return OutputStream for writing
     */
    @Override
    public PositionOutputStream create() {
        if (toInputFile().exists()) {
            throw new AlreadyExistsException("File already exists [%s]", path);
        }
        return createOrOverwrite();
    }

    /**
     * Create OutputStream or overwrite existing file when necessary
     *
     * @return OutputStream for writing
     */
    @Override
    public PositionOutputStream createOrOverwrite() {
        try {
            return new GoogleCloudStoragePositionOutputStream(httpClientProvider, storageProperties, location);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create stream [%s]".formatted(path), e);
        }
    }

    /**
     * Get Location of OutputFile
     *
     * @return Location
     */
    @Override
    public String location() {
        return path;
    }

    /**
     * Get InputFile corresponding to location of current OutputFile
     *
     * @return InputFile for current location
     */
    @Override
    public InputFile toInputFile() {
        return new GoogleCloudStorageInputFile(httpClientProvider, storageProperties, location, path, null);
    }
}
