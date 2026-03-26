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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Parsed representation of Google Cloud Storage URI
 */
record GoogleCloudStorageLocation(
        String bucket,
        String objectKey,
        String encodedObjectKey
) {

    private static final String GS_SCHEME = "gs://";

    private static final char FORWARD_SLASH = '/';

    private static final String PLUS = "+";

    private static final String SPACE_ENCODED = "%20";

    private static final String URI_FORMAT = "gs://%s/%s";

    /**
     * Parse URI to Google Cloud Storage Location
     *
     * @param uri Google Cloud Storage URI
     * @return Parsed Location
     */
    static GoogleCloudStorageLocation parse(final String uri) {
        if (uri == null) {
            throw new IllegalArgumentException("URI required");
        }

        if (uri.startsWith(GS_SCHEME)) {
            final String path = uri.substring(GS_SCHEME.length());
            final int forwardSlashIndex = path.indexOf(FORWARD_SLASH);
            if (forwardSlashIndex < 1) {
                throw new IllegalArgumentException("URI [%s] missing bucket and object objectKey".formatted(uri));
            }

            final String bucket = path.substring(0, forwardSlashIndex);
            final String objectKey = path.substring(forwardSlashIndex + 1);
            if (objectKey.isEmpty()) {
                throw new IllegalArgumentException("URI [%s] empty object objectKey".formatted(uri));
            }

            final String encodedObjectKey = getEncodedObjectKey(objectKey);
            return new GoogleCloudStorageLocation(bucket, objectKey, encodedObjectKey);
        } else {
            throw new IllegalArgumentException("URI [%s] missing required scheme [gs]".formatted(uri));
        }
    }

    private static String getEncodedObjectKey(final String objectKey) {
        final String urlEncodedObjectKey = URLEncoder.encode(objectKey, StandardCharsets.UTF_8);
        return urlEncodedObjectKey.replace(PLUS, SPACE_ENCODED);
    }

    @Override
    public String toString() {
        return URI_FORMAT.formatted(bucket, objectKey);
    }
}
