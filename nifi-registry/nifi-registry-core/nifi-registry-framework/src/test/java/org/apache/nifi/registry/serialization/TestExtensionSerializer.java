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
package org.apache.nifi.registry.serialization;

import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestExtensionSerializer {

    private Serializer<Extension> serializer;

    @Before
    public void setup() {
        serializer = new ExtensionSerializer();
    }

    @Test
    public void testSerializeAndDeserialize() {
        final Extension extension = new Extension();
        extension.setName("org.apache.nifi.FooProcessor");
        extension.setDescription("This is the foo processor");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        serializer.serialize(extension, outputStream);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final Extension deserialized = serializer.deserialize(inputStream);
        assertNotNull(deserialized);
        assertEquals(extension.getName(), deserialized.getName());
        assertEquals(extension.getDescription(), deserialized.getDescription());
    }
}
