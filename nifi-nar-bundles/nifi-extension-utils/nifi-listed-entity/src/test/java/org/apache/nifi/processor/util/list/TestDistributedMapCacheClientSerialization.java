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

package org.apache.nifi.processor.util.list;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.apache.nifi.processor.util.list.DistributedMapCacheClientSerialization.listedEntitiesDeserializer;
import static org.apache.nifi.processor.util.list.DistributedMapCacheClientSerialization.listedEntitiesSerializer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDistributedMapCacheClientSerialization {

    @Nested
    class RegardingListedEntitySerialization {
        @Test
        void canDeserializeListedEntitySerializedWithoutListingTimestamp() throws IOException {
            byte[] listedEntityEncodedBeforeListingTimestampIntroduction = new byte[]{
                    31, -117, 8, 0, 0, 0, 0, 0, 0, -1, -85, 86, 74, -51, 43, -55, 44, -87, -12, 76, 1, -47,
                    105, -103, -87, 69, 74, 86, -43, 74, 37, -103, -71, -87, -59, 37, -119, -71, 5, 74, 86,
                    -122, 70, -58, 38, -90, 58, 74, -59, -103, 85, -87, 74, 86, 102, -26, 22, -106, 6, -75,
                    -75, 0, -55, -14, -93, -58, 53, 0, 0, 0
            };

            Map<String, ListedEntity> result = listedEntitiesDeserializer
                    .deserialize(listedEntityEncodedBeforeListingTimestampIntroduction);

            assertNotNull(result);
            assertEquals(1, result.size());
            ListedEntity entity = result.get("entityIdentifier");
            assertNotNull(entity);
            assertEquals(12345L, entity.getTimestamp());
            assertEquals(67890L, entity.getSize());
            assertEquals(ListedEntity.NO_LISTING_TIMESTAMP, entity.getListingTimestamp());
        }

        @Test
        void canTestEmptyEntities() throws IOException {
            Map<String, ListedEntity> emptyEntities = Map.of();

            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                listedEntitiesSerializer.serialize(emptyEntities, outputStream);

                final Map<String, ListedEntity> result = listedEntitiesDeserializer.deserialize(outputStream.toByteArray());
                assertTrue(result.isEmpty());
            }
        }

        @Test
        void canSerializeEntitiesWithoutListingTimestamp() throws IOException {
            ListedEntity entity = new ListedEntity(333L, 666L);
            Map<String, ListedEntity> entities = Map.of("exampleKey", entity);

            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                listedEntitiesSerializer.serialize(entities, outputStream);

                final Map<String, ListedEntity> result = listedEntitiesDeserializer.deserialize(outputStream.toByteArray());
                assertEquals(1, result.size());
                final ListedEntity resultEntity = result.get("exampleKey");
                assertNotNull(resultEntity);
                assertEquals(entity.getTimestamp(), resultEntity.getTimestamp());
                assertEquals(entity.getSize(), resultEntity.getSize());
                assertEquals(ListedEntity.NO_LISTING_TIMESTAMP, resultEntity.getListingTimestamp());
            }
        }

        @Test
        void canSerializeEntitiesWithListingTimestamp() throws IOException {
            ListedEntity entity = new ListedEntity(333L, 666L, 999L);
            Map<String, ListedEntity> entities = Map.of("exampleKey", entity);

            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                listedEntitiesSerializer.serialize(entities, outputStream);

                final Map<String, ListedEntity> result = listedEntitiesDeserializer.deserialize(outputStream.toByteArray());
                assertEquals(1, result.size());
                final ListedEntity resultEntity = result.get("exampleKey");
                assertNotNull(resultEntity);
                assertEquals(entity.getTimestamp(), resultEntity.getTimestamp());
                assertEquals(entity.getSize(), resultEntity.getSize());
                assertEquals(entity.getListingTimestamp(), resultEntity.getListingTimestamp());
            }
        }
    }
}