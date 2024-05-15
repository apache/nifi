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

import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCouchbaseUtils {

    @Disabled("This test method requires a live Couchbase Server instance")
    @Test
    public void testDocumentTypesAndStringConversion() {
        try (final Cluster cluster = Cluster.connect("couchbase://192.168.99.100:8091", "couchbase", "b1password")) {
            final Bucket bucket = cluster.bucket("b1");
            final Collection collection = bucket.collection("c1");

            collection.upsert("JsonDocument", JsonObject.create().put("one", 1));
            collection.upsert("JsonArray", JsonArray.create().add(1).add(2).add(3));
            collection.upsert("JsonDouble", 0.123);
            collection.upsert("JsonString", "value");
            collection.upsert("JsonBoolean", true);
            collection.upsert("JsonLong", 123L);

            collection.upsert("RawJsonDocument", "value");
            collection.upsert("StringDocument", "value");

            collection.upsert("BinaryDocument", Unpooled.copiedBuffer("value".getBytes(StandardCharsets.UTF_8)));
            collection.upsert("ByteArrayDocument", "value".getBytes(StandardCharsets.UTF_8));

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
                final GetResult document = collection.get(expectation[0]);
                assertEquals(expectation[1], document.contentAsObject().getClass().getSimpleName());
                assertEquals(expectation[2], CouchbaseUtils.getStringContent(document.contentAsObject()));
            }

            final GetResult binaryDocument = collection.get("BinaryDocument");
            final String stringFromByteBuff = CouchbaseUtils.getStringContent(binaryDocument.contentAsObject());
            assertEquals("value", stringFromByteBuff);

            DecodingFailureException e = assertThrows(DecodingFailureException.class, () -> collection.get("JsonDocument"));
            assertTrue(e.getMessage().contains("Flags (0x2000000) indicate non-binary document for id JsonDocument"));
        }
    }
}
