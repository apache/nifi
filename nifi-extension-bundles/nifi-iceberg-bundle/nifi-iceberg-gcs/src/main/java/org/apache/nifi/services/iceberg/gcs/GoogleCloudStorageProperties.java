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

import java.util.Map;
import java.util.Objects;

import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.DECRYPTION_KEY;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.ENCRYPTION_KEY;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.READ_CHUNK_SIZE_BYTES;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.SERVICE_HOST;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.USER_PROJECT;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.WRITE_CHUNK_SIZE_BYTES;

/**
 * Google Cloud Storage Properties encapsulating value parsing and URI resolution
 */
class GoogleCloudStorageProperties {

    static final String DEFAULT_SERVICE_HOST = "https://storage.googleapis.com";
    static final int DEFAULT_WRITE_CHUNK_SIZE = 8 * 1024 * 1024;
    static final int DEFAULT_READ_CHUNK_SIZE = 2 * 1024 * 1024;
    static final int MINIMUM_CHUNK_SIZE = 256 * 1024;

    private static final String METADATA_URI_FORMAT = "%s/storage/v1/b/%s/o/%s";
    private static final String UPLOAD_URI_FORMAT = "%s/upload/storage/v1/b/%s/o?uploadType=resumable&name=%s";
    private static final String DOWNLOAD_URI_FORMAT = "%s/storage/v1/b/%s/o/%s?alt=media";
    private static final String USER_PROJECT_FIRST_FORMAT = "%s?userProject=%s";
    private static final String USER_PROJECT_ADDITIONAL_FORMAT = "%s&userProject=%s";

    private static final String ENCRYPTION_ALGORITHM = "AES256";

    private final String serviceHost;
    private final String userProject;
    private final String encryptionKey;
    private final String decryptionKey;
    private final int writeChunkSize;
    private final int readChunkSize;

    GoogleCloudStorageProperties(final Map<String, String> properties) {
        Objects.requireNonNull(properties, "Properties required");

        final String serviceHost = properties.get(SERVICE_HOST.getProperty());
        if (serviceHost == null || serviceHost.isBlank()) {
            this.serviceHost = DEFAULT_SERVICE_HOST;
        } else {
            this.serviceHost = serviceHost;
        }

        this.userProject = properties.get(USER_PROJECT.getProperty());
        this.encryptionKey = properties.get(ENCRYPTION_KEY.getProperty());
        this.decryptionKey = properties.get(DECRYPTION_KEY.getProperty());

        final String writeChunkSizeBytes = properties.get(WRITE_CHUNK_SIZE_BYTES.getProperty());
        if (writeChunkSizeBytes == null) {
            this.writeChunkSize = DEFAULT_WRITE_CHUNK_SIZE;
        } else {
            this.writeChunkSize = Integer.parseInt(writeChunkSizeBytes);
        }

        final String readChunkSizeBytes = properties.get(READ_CHUNK_SIZE_BYTES.getProperty());
        if (readChunkSizeBytes == null) {
            this.readChunkSize = DEFAULT_READ_CHUNK_SIZE;
        } else {
            this.readChunkSize = Integer.parseInt(readChunkSizeBytes);
        }
    }

    String encryptionAlgorithm() {
        return ENCRYPTION_ALGORITHM;
    }

    String encryptionKey() {
        return encryptionKey;
    }

    String decryptionKey() {
        return decryptionKey;
    }

    int writeChunkSize() {
        return writeChunkSize;
    }

    int readChunkSize() {
        return readChunkSize;
    }

    String metadataUri(final GoogleCloudStorageLocation location) {
        final String uri = METADATA_URI_FORMAT.formatted(serviceHost, location.bucket(), location.encodedObjectKey());

        final String metadataUri;
        if (userProject == null) {
            metadataUri = uri;
        } else {
            metadataUri = USER_PROJECT_FIRST_FORMAT.formatted(uri, userProject);
        }

        return metadataUri;
    }

    String uploadUri(final GoogleCloudStorageLocation location) {
        final String uri = UPLOAD_URI_FORMAT.formatted(serviceHost, location.bucket(), location.encodedObjectKey());

        final String uploadUri;
        if (userProject == null) {
            uploadUri = uri;
        } else {
            uploadUri = USER_PROJECT_ADDITIONAL_FORMAT.formatted(uri, userProject);
        }

        return uploadUri;
    }

    String downloadUri(final GoogleCloudStorageLocation location) {
        final String uri = DOWNLOAD_URI_FORMAT.formatted(serviceHost, location.bucket(), location.encodedObjectKey());

        final String downloadUri;
        if (userProject == null) {
            downloadUri = uri;
        } else {
            downloadUri = USER_PROJECT_ADDITIONAL_FORMAT.formatted(uri, userProject);
        }

        return downloadUri;
    }
}
