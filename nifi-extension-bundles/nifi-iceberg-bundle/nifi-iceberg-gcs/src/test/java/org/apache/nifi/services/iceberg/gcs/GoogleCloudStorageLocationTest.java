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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GoogleCloudStorageLocationTest {

    private static final String OBJECT_BUCKET = "object-bucket";
    private static final String SIMPLE_KEY = "path/to/object.parquet";
    private static final String OBJECT_URI = "gs://%s/%s".formatted(OBJECT_BUCKET, SIMPLE_KEY);
    private static final String BUCKET = "bucket";
    private static final String OBJECT_PARQUET = "object.parquet";
    private static final String SINGLE_SEGMENT_URI = "gs://%s/%s".formatted(BUCKET, OBJECT_PARQUET);
    private static final String DEEP_KEY = "db/table/data/00000-0-abc.parquet";
    private static final String DEEP_PATH_URI = "gs://bucket/%s".formatted(DEEP_KEY);

    private static final String MULTI_SEGMENT_URI = "gs://bucket/a/b/c";
    private static final String SPACES_URI = "gs://bucket/path with spaces/file name.txt";
    private static final String ENCODED_MULTI_SEGMENT = "a%2Fb%2Fc";
    private static final String ENCODED_SPACES = "path%20with%20spaces%2Ffile%20name.txt";

    private static final String SIMPLE_BUCKET_KEY_URI = "gs://bucket/objectKey";
    private static final String INVALID_SCHEME_URI = "s3://bucket/objectKey";
    private static final String NO_BUCKET_URI = "gs:///objectKey";
    private static final String NO_KEY_URI = "gs://bucket/";
    private static final String NO_SLASH_URI = "gs://bucket";

    @Test
    void testParseSimple() {
        final GoogleCloudStorageLocation location = GoogleCloudStorageLocation.parse(OBJECT_URI);
        assertEquals(OBJECT_BUCKET, location.bucket());
        assertEquals(SIMPLE_KEY, location.objectKey());
    }

    @Test
    void testParseSingleSegmentKey() {
        final GoogleCloudStorageLocation location = GoogleCloudStorageLocation.parse(SINGLE_SEGMENT_URI);
        assertEquals(BUCKET, location.bucket());
        assertEquals(OBJECT_PARQUET, location.objectKey());
    }

    @Test
    void testParseDeepPath() {
        final GoogleCloudStorageLocation location = GoogleCloudStorageLocation.parse(DEEP_PATH_URI);
        assertEquals(BUCKET, location.bucket());
        assertEquals(DEEP_KEY, location.objectKey());
    }

    @Test
    void testEncodedKeyEncodesSlashes() {
        final GoogleCloudStorageLocation location = GoogleCloudStorageLocation.parse(MULTI_SEGMENT_URI);
        assertEquals(ENCODED_MULTI_SEGMENT, location.encodedObjectKey());
    }

    @Test
    void testEncodedKeyEncodesSpaces() {
        final GoogleCloudStorageLocation location = GoogleCloudStorageLocation.parse(SPACES_URI);
        assertEquals(ENCODED_SPACES, location.encodedObjectKey());
    }

    @Test
    void testToString() {
        final GoogleCloudStorageLocation location = GoogleCloudStorageLocation.parse(SIMPLE_BUCKET_KEY_URI);
        assertEquals(SIMPLE_BUCKET_KEY_URI, location.toString());
    }

    @Test
    void testParseNullThrows() {
        assertThrows(IllegalArgumentException.class, () -> GoogleCloudStorageLocation.parse(null));
    }

    @Test
    void testParseInvalidSchemeThrows() {
        assertThrows(IllegalArgumentException.class, () -> GoogleCloudStorageLocation.parse(INVALID_SCHEME_URI));
    }

    @Test
    void testParseNoBucketThrows() {
        assertThrows(IllegalArgumentException.class, () -> GoogleCloudStorageLocation.parse(NO_BUCKET_URI));
    }

    @Test
    void testParseNoKeyThrows() {
        assertThrows(IllegalArgumentException.class, () -> GoogleCloudStorageLocation.parse(NO_KEY_URI));
    }

    @Test
    void testParseNoSlashThrows() {
        assertThrows(IllegalArgumentException.class, () -> GoogleCloudStorageLocation.parse(NO_SLASH_URI));
    }
}
