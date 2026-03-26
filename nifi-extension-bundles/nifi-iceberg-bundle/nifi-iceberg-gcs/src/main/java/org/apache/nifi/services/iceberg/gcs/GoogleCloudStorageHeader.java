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

/**
 * HTTP header names used in Google Cloud Storage JSON API requests and responses
 */
enum GoogleCloudStorageHeader {
    /** RFC 9110 Section 11.6.2 */
    AUTHORIZATION("Authorization"),

    /** RFC 9110 Section 14.4 */
    CONTENT_RANGE("Content-Range"),

    /** Google Header for customer-supplied encryption keys */
    ENCRYPTION_ALGORITHM("x-goog-encryption-algorithm"),

    /** Google Header for customer-supplied encryption keys containing Base64-encoded values */
    ENCRYPTION_KEY("x-goog-encryption-key"),

    /** Google Header for customer-supplied encryption keys containing Base64-encoded values */
    ENCRYPTION_KEY_SHA256("x-goog-encryption-key-sha256"),

    /** RFC 9110 Section 10.2.2 */
    LOCATION("Location"),

    /** RFC 9110 Section 14.2 */
    RANGE("Range"),

    /** Content Type for payload of Google Cloud Storage resumable uploads */
    UPLOAD_CONTENT_TYPE("X-Upload-Content-Type");

    private final String header;

    GoogleCloudStorageHeader(final String header) {
        this.header = header;
    }

    String getHeader() {
        return header;
    }
}
