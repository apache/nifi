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
package org.apache.nifi.processors.standard.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.junit.Assert;
import org.junit.Test;

public class TestFlowFileAttributesSerializer {

    private FlowFileAttributesSerializer serializer = new FlowFileAttributesSerializer();

    @Test
    public void testBothWays() throws SerializationException, IOException {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "1");
        attributes.put("b", "2");

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.serialize(attributes, output);
        output.flush();

        Map<String, String> result = serializer.deserialize(output.toByteArray());
        Assert.assertEquals(attributes, result);
    }

    @Test
    public void testEmptyIsNull() throws SerializationException, IOException {
        Map<String, String> attributes = new HashMap<>();

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.serialize(attributes, output);
        output.flush();

        Map<String, String> result = serializer.deserialize(output.toByteArray());
        Assert.assertNull(result);
    }

    @Test
    public void testEmptyIsNull2() throws SerializationException, IOException {
        Map<String, String> result = serializer.deserialize("".getBytes());
        Assert.assertNull(result);
    }

    @Test
    public void testNullIsNull() throws SerializationException, IOException {
        Map<String, String> result = serializer.deserialize(null);
        Assert.assertNull(result);
    }
}