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
package org.apache.nifi.couchbase;

import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.ByteArrayDocument;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.JsonBooleanDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonDoubleDocument;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.document.JsonStringDocument;
import com.couchbase.client.java.document.LegacyDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TranscodingException;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCouchbaseUtils {

    @Ignore("This test method requires a live Couchbase Server instance")
    @Test
    public void testDocumentTypesAndStringConversion() {
        final CouchbaseCluster cluster = CouchbaseCluster.fromConnectionString("couchbase://192.168.99.100:8091");
        final Bucket bucket = cluster.openBucket("b1", "b1password");

        bucket.upsert(JsonDocument.create("JsonDocument", JsonObject.create().put("one", 1)));
        bucket.upsert(JsonArrayDocument.create("JsonArray", JsonArray.create().add(1).add(2).add(3)));
        bucket.upsert(JsonDoubleDocument.create("JsonDouble", 0.123));
        bucket.upsert(JsonStringDocument.create("JsonString", "value"));
        bucket.upsert(JsonBooleanDocument.create("JsonBoolean", true));
        bucket.upsert(JsonLongDocument.create("JsonLong", 123L));

        bucket.upsert(RawJsonDocument.create("RawJsonDocument", "value"));
        bucket.upsert(StringDocument.create("StringDocument", "value"));

        bucket.upsert(BinaryDocument.create("BinaryDocument", Unpooled.copiedBuffer("value".getBytes(StandardCharsets.UTF_8))));
        bucket.upsert(ByteArrayDocument.create("ByteArrayDocument", "value".getBytes(StandardCharsets.UTF_8)));

        final String[][] expectations = {
                {"JsonDocument", "String", "{\"one\":1}"},
                {"JsonArray", "String", "[1,2,3]"},
                {"JsonDouble", "String", "0.123"},
                {"JsonString", "String", "\"value\""},
                {"JsonBoolean", "String", "true"},
                {"JsonLong", "String", "123"},
                {"RawJsonDocument", "String", "value"},
                {"StringDocument", "String", "value"},
                {"BinaryDocument", "byte[]", "value"},
                {"ByteArrayDocument", "byte[]", "value"},
        };

        for (String[] expectation : expectations) {
            final LegacyDocument document = bucket.get(LegacyDocument.create(expectation[0]));
            assertEquals(expectation[1], document.content().getClass().getSimpleName());
            assertEquals(expectation[2], CouchbaseUtils.getStringContent(document.content()));
        }

        final BinaryDocument binaryDocument = bucket.get(BinaryDocument.create("BinaryDocument"));
        final String stringFromByteBuff = CouchbaseUtils.getStringContent(binaryDocument.content());
        assertEquals("value", stringFromByteBuff);

        try {
            bucket.get(BinaryDocument.create("JsonDocument"));
            fail("Getting a JSON document as a BinaryDocument fails");
        } catch (TranscodingException e) {
            assertTrue(e.getMessage().contains("Flags (0x2000000) indicate non-binary document for id JsonDocument"));
        }

    }
}
