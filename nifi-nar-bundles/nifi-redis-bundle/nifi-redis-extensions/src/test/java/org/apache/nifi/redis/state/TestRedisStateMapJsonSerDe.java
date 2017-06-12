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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestRedisStateMapJsonSerDe {

    private RedisStateMapSerDe serDe;

    @Before
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
        Assert.assertNotNull(serialized);

        final RedisStateMap deserialized = serDe.deserialize(serialized);
        Assert.assertNotNull(deserialized);
        Assert.assertEquals(stateMap.getVersion(), deserialized.getVersion());
        Assert.assertEquals(stateMap.getEncodingVersion(), deserialized.getEncodingVersion());
        Assert.assertEquals(stateMap.toMap(), deserialized.toMap());
    }

    @Test
    public void testSerializeWhenNull() throws IOException {
        Assert.assertNull(serDe.serialize(null));
    }

    @Test
    public void testDeserializeWhenNull() throws IOException {
        Assert.assertNull(serDe.deserialize(null));
    }

    @Test
    public void testDefaultSerialization() throws IOException {
        final RedisStateMap stateMap = new RedisStateMap.Builder().build();

        final byte[] serialized = serDe.serialize(stateMap);
        Assert.assertNotNull(serialized);

        final RedisStateMap deserialized = serDe.deserialize(serialized);
        Assert.assertNotNull(deserialized);
        Assert.assertEquals(RedisStateMap.DEFAULT_VERSION.longValue(), stateMap.getVersion());
        Assert.assertEquals(RedisStateMap.DEFAULT_ENCODING, stateMap.getEncodingVersion());
        Assert.assertNotNull(deserialized.toMap());
        Assert.assertEquals(0, deserialized.toMap().size());
    }

}
