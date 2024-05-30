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
package org.apache.nifi.processors.azure.storage.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.NullSuppression;
import org.apache.nifi.json.OutputGrouping;
import org.apache.nifi.json.WriteJsonResult;
import org.apache.nifi.schema.access.NopSchemaAccessWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BlobInfoTest {

    private static final String BLOB_NAME_VALUE = "blob_name";
    private static final String BLOB_TYPE_VALUE = "blob_type";
    private static final String CONTAINER_NAME_VALUE = "container_name";
    private static final long LENGTH_VALUE = 10;
    private static final long LAST_MODIFIED_VALUE = 12345;
    private static final String ETAG_VALUE = "etag";
    private static final String CONTENT_LANGUAGE_VALUE = "content_language";
    private static final String CONTENT_TYPE_VALUE = "content_type";
    private static final String PRIMARY_URI_VALUE = "primary_uri/" + BLOB_NAME_VALUE;
    private static final String SECONDARY_URI_VALUE = "secondary_uri/" + BLOB_NAME_VALUE;

    @Test
    void testBlobInfo() throws IOException {
        final BlobInfo blobInfo = new BlobInfo.Builder()
                .blobName(BLOB_NAME_VALUE)
                .blobType(BLOB_TYPE_VALUE)
                .containerName(CONTAINER_NAME_VALUE)
                .length(LENGTH_VALUE)
                .lastModifiedTime(LAST_MODIFIED_VALUE)
                .etag(ETAG_VALUE)
                .contentLanguage(CONTENT_LANGUAGE_VALUE)
                .contentType(CONTENT_TYPE_VALUE)
                .primaryUri(PRIMARY_URI_VALUE)
                .secondaryUri(SECONDARY_URI_VALUE)
                .build();

        final Record record = blobInfo.toRecord();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final RecordSetWriter recordSetWriter = new WriteJsonResult(
                new MockComponentLog("test_id", "test_component"),
                BlobInfo.getRecordSchema(),
                new NopSchemaAccessWriter(),
                baos,
                false,
                NullSuppression.ALWAYS_SUPPRESS,
                OutputGrouping.OUTPUT_ONELINE,
                null,
                null,
                null
                );

        recordSetWriter.write(record);
        recordSetWriter.flush();

        final JsonNode json = new ObjectMapper().readTree(baos.toString());

        assertEquals(BLOB_NAME_VALUE, json.get(BlobInfo.BLOB_NAME).asText());
        assertEquals(BLOB_NAME_VALUE, json.get(BlobInfo.FILENAME).asText());
        assertEquals(BLOB_TYPE_VALUE, json.get(BlobInfo.BLOB_TYPE).asText());
        assertEquals(CONTAINER_NAME_VALUE, json.get(BlobInfo.CONTAINER_NAME).asText());
        assertEquals(LENGTH_VALUE, json.get(BlobInfo.LENGTH).asLong());
        assertEquals(LAST_MODIFIED_VALUE, json.get(BlobInfo.LAST_MODIFIED).asLong());
        assertEquals(ETAG_VALUE, json.get(BlobInfo.ETAG).asText());
        assertEquals(CONTENT_LANGUAGE_VALUE, json.get(BlobInfo.CONTENT_LANGUAGE).asText());
        assertEquals(CONTENT_TYPE_VALUE, json.get(BlobInfo.CONTENT_TYPE).asText());
        assertEquals(PRIMARY_URI_VALUE, json.get(BlobInfo.PRIMARY_URI).asText());
        assertEquals(SECONDARY_URI_VALUE, json.get(BlobInfo.SECONDARY_URI).asText());
    }
}
