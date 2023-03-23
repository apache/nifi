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
package org.apache.nifi.redis.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestRedisStateMapJsonSerDe {

    private RedisStateMapSerDe serDe;

    @BeforeEach
    public void setup() {
        serDe = new RedisStateMapJsonSerDe();
    }

    @Test
    public void testSerializeDeserialize() throws IOException {
        final RedisStateMap stateMap = new RedisStateMap.Builder()
                .version(2L)
                .encodingVersion(3)
                .stateValue("field1", "value1")
                .stateValue("field2", "value2")
                .stateValue("field3", "value3")
                .build();

        final byte[] serialized = serDe.serialize(stateMap);
        assertNotNull(serialized);

        final RedisStateMap deserialized = serDe.deserialize(serialized);
        assertNotNull(deserialized);
        assertEquals(stateMap.getStateVersion(), deserialized.getStateVersion());
        assertEquals(stateMap.getEncodingVersion(), deserialized.getEncodingVersion());
        assertEquals(stateMap.toMap(), deserialized.toMap());
    }

    @Test
    public void testSerializeWhenNull() throws IOException {
        assertNull(serDe.serialize(null));
    }

    @Test
    public void testDeserializeWhenNull() throws IOException {
        assertNull(serDe.deserialize(null));
    }

    @Test
    public void testDefaultSerialization() throws IOException {
        final RedisStateMap stateMap = new RedisStateMap.Builder().build();

        final byte[] serialized = serDe.serialize(stateMap);
        assertNotNull(serialized);

        final RedisStateMap deserialized = serDe.deserialize(serialized);
        assertNotNull(deserialized);
        assertFalse(stateMap.getStateVersion().isPresent());
        assertEquals(RedisStateMap.DEFAULT_ENCODING, stateMap.getEncodingVersion());
        assertNotNull(deserialized.toMap());
        assertEquals(0, deserialized.toMap().size());
    }

}
